package main

import (
	"context"
	"database/sql"
	"log"
	"time"

	"billing-service/consumer"
	_ "github.com/lib/pq"
)

func main() {
	amqpURI := "amqp://admin_medcore:SecuredPassRabbit2026!@localhost:5672/%2Fmedcore_vhost"
	dbURI := "postgres://usr_billing_svc:SecuredPassBilling2026!@localhost:3307/medcore_billing_db?sslmode=disable"

	// Koneksi ke PostgreSQL
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	db, err := sql.Open("postgres", dbURI)
	if err != nil {
		log.Fatalf("[Billing Service] Gagal koneksi PostgreSQL: %v", err)
	}
	defer db.Close()

	if err := db.PingContext(ctx); err != nil {
		log.Fatalf("[Billing Service] Gagal ping PostgreSQL: %v", err)
	}
	log.Println("[Billing Service] Berhasil terhubung ke PostgreSQL medcore_billing_db")

	// Jalankan Consumer
	consumer.StartBillingConsumer(amqpURI, db)
}