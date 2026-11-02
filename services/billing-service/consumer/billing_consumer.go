package consumer

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	_ "github.com/lib/pq"
)

// EventEnvelope adalah struktur payload yang diterima dari Appointment Service
type EventEnvelope struct {
	EventID string `json:"event_id"`
	Payload struct {
		AppointmentID string `json:"appointment_id"`
		PatientID     string `json:"patient_id"`
		DoctorID      string `json:"doctor_id"`
		BillingSummary struct {
			ConsultationFee float64 `json:"consultation_fee"`
			TriageUrgency   string  `json:"triage_urgency"`
		} `json:"billing_summary"`
	} `json:"payload"`
}

// ProcessedEventRecord untuk Idempotency Ledger
type ProcessedEventRecord struct {
	EventID     string    `json:"event_id"`
	ProcessedAt time.Time `json:"processed_at"`
	Status      string    `json:"status"`
}

// StartBillingConsumer memulai consumer RabbitMQ untuk Billing Service
func StartBillingConsumer(amqpURI string, db *sql.DB) {
	conn, err := amqp.Dial(amqpURI)
	if err != nil {
		log.Fatalf("[Billing Consumer] Gagal koneksi broker: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("[Billing Consumer] Gagal membuka channel: %v", err)
	}
	defer ch.Close()

	// 1. Deklarasikan Queue durable dengan DLX
	args := amqp.Table{
		"x-dead-letter-exchange": "medcore.deadletter.exchange",
	}
	q, err := ch.QueueDeclare(
		"billing.invoice.q",
		true,  // Durable
		false, // Auto-delete
		false, // Exclusive
		false, // No-wait
		args,
	)
	if err != nil {
		log.Fatalf("[Billing Consumer] Gagal deklarasi queue: %v", err)
	}

	// 2. Ikat Queue ke Exchange
	err = ch.QueueBind(
		q.Name,
		"clinical.consultation.completed",
		"medcore.clinical.events",
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("[Billing Consumer] Gagal binding queue: %v", err)
	}

	// 3. Set QoS = 10 (mencegah OOM)
	err = ch.Qos(10, 0, false)
	if err != nil {
		log.Fatalf("[Billing Consumer] Gagal set QoS: %v", err)
	}

	// 4. Consume dengan Manual ACK
	msgs, err := ch.Consume(
		q.Name,
		"billing-worker",
		false, // autoAck = FALSE
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("[Billing Consumer] Gagal subscribe: %v", err)
	}

	log.Printf("[Billing Consumer] Menunggu event konsultasi selesai pada antrean: %s", q.Name)
	forever := make(chan bool)

	go func() {
		for d := range msgs {
			var evt EventEnvelope
			if err := json.Unmarshal(d.Body, &evt); err != nil {
				log.Printf("[Billing Consumer] Format pesan tidak valid, NACK tanpa requeue: %v", err)
				d.Nack(false, false) // Langsung ke DLQ
				continue
			}

			log.Println("---------------------------------------------------------")
			log.Printf("[Billing Consumer] Menerima event konsultasi: %s", evt.EventID)
			log.Printf(" Appointment ID  : %s", evt.Payload.AppointmentID)
			log.Printf(" Patient ID      : %s", evt.Payload.PatientID)

			// Simulasi Poison Message (jika biaya negatif)
			if evt.Payload.BillingSummary.ConsultationFee < 0 {
				log.Printf("[Billing Consumer] Terdeteksi Poison Message! Biaya negatif: %.2f. Mengirim NACK ke DLQ.", evt.Payload.BillingSummary.ConsultationFee)
				d.Nack(false, false) // requeue = FALSE -> DLX
				log.Println("---------------------------------------------------------")
				continue
			}

			// Idempotensi (Cek Duplikasi di PostgreSQL)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

			var exists bool
			err := db.QueryRowContext(ctx,
				`SELECT EXISTS(SELECT 1 FROM processed_events WHERE event_id = $1)`,
				evt.EventID,
			).Scan(&exists)
			if err != nil {
				log.Printf("[Billing Consumer] Gagal cek DB: %v, requeue", err)
				d.Nack(false, true)
				cancel()
				continue
			}
			if exists {
				log.Printf("[Deduplication Guard] Event %s terdeteksi duplikat! Melewati eksekusi bisnis dan langsung mengirim ACK.", evt.EventID)
				cancel()
				d.Ack(false)
				log.Println("---------------------------------------------------------")
				continue
			}

			// Logika Bisnis: Membuat Invoice
			log.Printf("[Business Logic] Membuat invoice untuk Appointment: %s", evt.Payload.AppointmentID)

			// Transaksi Database
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				log.Printf("[Billing Consumer] Gagal begin tx: %v", err)
				d.Nack(false, true)
				cancel()
				continue
			}
			defer tx.Rollback()

			// Insert ke tabel invoices
			_, err = tx.ExecContext(ctx,
				`INSERT INTO invoices (invoice_id, appointment_id, patient_id, amount, status, created_at)
				 VALUES ($1, $2, $3, $4, 'PENDING', NOW())`,
				"INV-"+evt.EventID, // invoice_id unik
				evt.Payload.AppointmentID,
				evt.Payload.PatientID,
				evt.Payload.BillingSummary.ConsultationFee,
			)
			if err != nil {
				log.Printf("[Billing Consumer] Gagal insert invoice: %v", err)
				d.Nack(false, true)
				cancel()
				continue
			}

			// Catat Event ID ke processed_events
			_, err = tx.ExecContext(ctx,
				`INSERT INTO processed_events (event_id, processed_at, status)
				 VALUES ($1, NOW(), 'SUCCESS')`,
				evt.EventID,
			)
			if err != nil {
				log.Printf("[Billing Consumer] Gagal catat processed_events: %v", err)
				d.Nack(false, true)
				cancel()
				continue
			}

			if err := tx.Commit(); err != nil {
				log.Printf("[Billing Consumer] Gagal commit: %v", err)
				d.Nack(false, true)
				cancel()
				continue
			}

			cancel()
			d.Ack(false)
			log.Printf("[Billing Consumer] Invoice untuk Appointment %s berhasil dibuat dan di-ACK.", evt.Payload.AppointmentID)
			log.Println("---------------------------------------------------------")
		}
	}()

	<-forever
}