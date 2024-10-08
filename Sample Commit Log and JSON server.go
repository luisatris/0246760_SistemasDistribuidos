package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
)

const port string = ":8080"

type Log struct {
	Record string `json:"record"`
	Offset int    `json:"offset"`
}

type Response struct {
	Mensaje string `json:"mensaje"`
	Offset  int    `json:"offset"`
}

var logs []Log
var mu sync.Mutex

func main() {
	fmt.Println("Iniciando servicio")
	http.HandleFunc("/escribir", EscribirLog)
	http.HandleFunc("/leer", LeerLog)
	fmt.Println("Servidor funcionando en el puerto", port)
	err := http.ListenAndServe(port, nil)
	if err != nil {
		fmt.Println("Error iniciando el servidor:", err)
	}
}

func EscribirLog(w http.ResponseWriter, r *http.Request) {
	var entry Log
	err := json.NewDecoder(r.Body).Decode(&entry)
	if err != nil {
		http.Error(w, "Error al procesar el JSON", http.StatusBadRequest)
		return
	}

	// Lock para proteger el acceso a logs
	mu.Lock()
	entry.Offset = len(logs)
	logs = append(logs, entry)
	mu.Unlock()

	w.WriteHeader(http.StatusCreated)
	response := Response{
		Mensaje: "Log escrito correctamente",
		Offset:  entry.Offset,
	}
	json.NewEncoder(w).Encode(response)
}

func LeerLog(w http.ResponseWriter, r *http.Request) {
	var request struct {
		Offset int `json:"offset"`
	}
	err := json.NewDecoder(r.Body).Decode(&request)
	if err != nil {
		http.Error(w, "Error al procesar el JSON", http.StatusBadRequest)
		return
	}

	// Lock para proteger el acceso a logs
	mu.Lock()
	defer mu.Unlock()

	// si se pide un offset fuera de los rangos ingresados regresa un error
	if request.Offset < 0 || request.Offset >= len(logs) {
		http.Error(w, "Offset fuera de rango", http.StatusBadRequest)
		return
	}
	entry := logs[request.Offset]

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(entry)
}
