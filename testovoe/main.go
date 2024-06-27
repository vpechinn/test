package main

import (
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	_ "github.com/lib/pq"
)

var db *sql.DB

type DepthOrder struct {
	Price  float64 `json:"price"`
	Amount float64 `json:"amount"`
}

type HistoryOrder struct {
	OrderID  int       `json:"price"`
	ClientID int       `json:"client_id"`
	Order    string    `json:"order"`
	Created  time.Time `json:"created_at"`
}

type Clinet struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

func initDB() {
	var err error
	connStr := "user=userName dbname=dbname sslmode=disable"

	db, err = sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}
}

func GetOrderBook(w http.ResponseWriter, r *http.Request) {
	exchangeName := r.URL.Query().Get("exchange_name")
	pair := r.URL.Query().Get("pair")

	if exchangeName == "" || pair == "" {
		http.Error(w, "Missing parameters", http.StatusBadRequest)
		return
	}

	rows, err := db.Query("SELECT depth FROM order_books WHERE exchange_name = $1 AND pair = $2", exchangeName, pair)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var orderBooks []*DepthOrder
	for rows.Next() {
		var depthJSON string
		if err := rows.Scan(&depthJSON); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	var depth []*DepthOrder
	if err := json.Unmarshal([]byte(depthJSON), &depth); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	orderBooks = append(orderBooks, depth...)
	if err := rows.Err(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(orderBooks)
}

func SaveOrderBook(w http.ResponseWriter, r *http.Request) {
	var orderBook struct {
		ExchangeName string        `json:"exchange_name"`
		Pair         string        `json:"pair"`
		Depth        []*DepthOrder `json:"depth"`
	}

	if err := json.NewDecoder(r.Body).Decode(&orderBook); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	depthJSON, err := json.Marshal(orderBook.Depth)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	_, err = db.Exec("INSERT INTO order_books (exchange_name, pair, depth, created_at) VALUES (?,?,?,?)",
		orderBook.ExchangeName, orderBook.Pair, string(depthJSON), time.Now())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
}

func GetOrderHistory(w http.ResponseWriter, r *http.Request) {
	clieintID := r.URL.Query().Get("client_id")

	if clieintID == "" {
		http.Error(w, "Missing client_id parametr", http.StatusBadRequest)
		return
	}

	rows, err := db.Query("SELECT client_id, order, created_at FROM history_orders WHERE client_id = ?", clieintID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var historyOrders []*HistoryOrder

	for rows.Next() {
		var historyOrder HistoryOrder
		if err := rows.Scan(&historyOrder.ClientID, &historyOrder, &historyOrder.Created); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		historyOrders = append(historyOrders, &historyOrder)
	}

	if err := rows.Err(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(historyOrders)
}

func SaveOrder(w http.ResponseWriter, r *http.Request) {
	var order struct {
		ClientID int    `json:"client_id"`
		Order    string `json:"order"`
	}

	if err := json.NewDecoder(r.Body).Decode(&order); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	_, err := db.Exec("INSERT INTO history_orders (client_id, order, created_at) VALUES (?,?, ?)",
		order.ClientID, order.Order, time.Now())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
}

func main() {
	initDB()

	defer db.Close()

	r := mux.NewRouter()
	r.HandleFunc("/orderbook", GetOrderBook).Methods("Get")
	r.HandleFunc("/orderbook", SaveOrderBook).Methods("POST")
	r.HandleFunc("/orderhistor", GetOrderHistory).Methods("GET")
	r.HandleFunc("/order", SaveOrder).Methods("POST")

	log.Fatal(http.ListenAndServe(":8080", r))
}
