package main

import (
	"ProyectoParcial1Go/internal/server"
	"log"
)

func main() {
	// Inicia el servidor HTTP en el puerto 8080
	srv := server.NewHTTPServer(":8080")
	log.Println("Servidor iniciado en el puerto 8080")

	// Usa log.Fatal para manejar cualquier error en el servidor
	log.Fatal(srv.ListenAndServe())
}
