package main

import (
	"encoding/json"
	"fmt"
	"net/http"
)

type User struct {
	Nombre string `json:"nombre"`
	Mail   string `json:"mail"`
}

var users []User

func main() {
	http.HandleFunc("/AgregarUsuario", AgregarUsuario)
	http.HandleFunc("/ObtenerUsuario", ObtenerUsuario)

	fmt.Println("Puerto 8080 funcionando")
	http.ListenAndServe(":8080", nil)
	if err := http.ListenAndServe(":8080", nil); err != nil {
		fmt.Println("Error al iniciar:", err)
	}
}

func AgregarUsuario(w http.ResponseWriter, r *http.Request) {
	var user User
	err := json.NewDecoder(r.Body).Decode(&user)
	if err != nil {
		http.Error(w, "Error al procesar el archivo", http.StatusBadRequest)
		return
	}

	users = append(users, user)

	w.WriteHeader(http.StatusCreated)
	fmt.Fprintf(w, "Usuario agregado correctamente")
}

func ObtenerUsuario(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(users)
}

//ingresar datos del usuario
//curl -X POST http://localhost:8080/AgregarUsuario -d '{"nombre":"Luis","mail":"luis@gmail.com"}' -H "Content-Type: application/json"
//obtener datos de los usuarios
//curl http://localhost:8080/ObtenerUsuario
