package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"

	"github.com/bmlott27/gogeo/postgres"
	"github.com/bmlott27/gogeo/utilities"
	"github.com/gorilla/mux"
	_ "github.com/lib/pq"
)

type County struct {
	Id       string       `json:"id"`
	CountyFP string       `json:"countyfp"`
	Geom     MultiPolygon `json:"geom"`
}

type MultiPolygon struct {
	Type        string           `json:"type"`
	Coordinates [][][][2]float64 `json:"coordinates"`
}

type JsonResponse struct {
	Type    string   `json:"type"`
	Data    []County `json:"data"`
	Message string   `json:"message"`
}

type NewCounty struct {
	CountyFP string       `json:"countyfp"`
	Geom     MultiPolygon `json:"geom"`
}

// Main function
func main() {

	// Init the mux router
	router := mux.NewRouter()

	// Route handles & endpoints

	// Get all counties
	router.HandleFunc("/counties/", GetCounties).Methods("GET")

	// Get a county by its id
	router.HandleFunc("/counties/{countyid}", GetCounty).Methods("GET")

	// Insert a county
	router.HandleFunc("/counties/", InsertCounty).Methods("POST")

	// Update a county
	router.HandleFunc("/counties/{countyid}", UpdateCounty).Methods("PUT")

	// Delete a county by its id
	router.HandleFunc("/counties/{countyid}", DeleteCounty).Methods("DELETE")

	// serve the app
	fmt.Println("Server at 8080")
	log.Fatal(http.ListenAndServe(":8080", router))
}

// common method for creating return json
func processRows(rows *sql.Rows) JsonResponse {
	// var response []JsonResponse
	var counties []County

	// Foreach movie
	for rows.Next() {
		var id string
		var countyFP string
		var geom string

		err := rows.Scan(&id, &countyFP, &geom)

		// check errors
		utilities.CheckErr(err)
		geoJson := MultiPolygon{}
		json.Unmarshal([]byte(geom), &geoJson)

		counties = append(counties, County{Id: id, CountyFP: countyFP, Geom: geoJson})
	}

	return JsonResponse{Type: "success", Data: counties, Message: fmt.Sprintf("Returned %s counties", strconv.Itoa(len(counties)))}
}

// Get counties
func GetCounties(w http.ResponseWriter, r *http.Request) {
	db := postgres.Connect()

	fmt.Println("Getting counties...")

	// Get all movies from movies table that don't have movieID = "1"
	rows, err := db.Query(`SELECT id, "COUNTYFP", ST_AsGeoJson(geom) FROM al_counties_wgs84`)

	// check errors
	utilities.CheckErr(err)

	response := processRows(rows)
	json.NewEncoder(w).Encode(response)
}

// Get a single county
func GetCounty(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)

	Id := params["countyid"]

	var response = JsonResponse{}

	if Id == "" {
		response = JsonResponse{Type: "error", Message: "You are missing county id parameter."}
	} else {
		db := postgres.Connect()

		fmt.Println("Getting county")

		rows, err := db.Query(`SELECT id, "COUNTYFP", ST_AsGeoJson(geom) FROM al_counties_wgs84 WHERE id = $1`, Id)

		// check errors
		utilities.CheckErr(err)

		response = processRows(rows)
	}

	json.NewEncoder(w).Encode(response)
}

// Create a county
func InsertCounty(w http.ResponseWriter, r *http.Request) {

	var newCounty NewCounty
	err := json.NewDecoder(r.Body).Decode(&newCounty)
	utilities.CheckErr(err)

	var response = JsonResponse{}

	if err != nil {
		response = JsonResponse{Type: "error", Message: "You are missing countyfp or geom parameter."}
	} else {
		db := postgres.Connect()

		fmt.Println("Inserting county")

		var lastInsertID int
		geoJson, err := json.Marshal(newCounty.Geom)
		utilities.CheckErr(err)

		err = db.QueryRow(`INSERT INTO al_counties_wgs84("COUNTYFP", geom) VALUES($1, ST_GeomFromGeoJson($2)) returning id;`, newCounty.CountyFP, geoJson).Scan(&lastInsertID)

		// check errors
		utilities.CheckErr(err)

		response = JsonResponse{Type: "success", Message: fmt.Sprintf("The county has been inserted successfully! New id: %s", strconv.Itoa(lastInsertID))}
	}

	json.NewEncoder(w).Encode(response)
}

func UpdateCounty(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)

	Id := params["countyid"]

	var newCounty NewCounty
	err := json.NewDecoder(r.Body).Decode(&newCounty)
	utilities.CheckErr(err)

	var response = JsonResponse{}

	if Id == "" {
		response = JsonResponse{Type: "error", Message: "You are missing county id parameter."}
	} else {
		db := postgres.Connect()

		fmt.Println("Updating county")
		geoJson, err := json.Marshal(newCounty.Geom)
		utilities.CheckErr(err)

		_, err = db.Exec(`UPDATE al_counties_wgs84 SET "COUNTYFP" = $1, geom = ST_GeomFromGeoJson($2) WHERE id = $3`, newCounty.CountyFP, geoJson, Id)

		// check errors
		utilities.CheckErr(err)

		response = JsonResponse{Type: "success", Message: fmt.Sprintf("The county has been updated successfully! Id: %s", Id)}
	}

	json.NewEncoder(w).Encode(response)
}

// response and request handlers
func DeleteCounty(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)

	Id := params["countyid"]

	var response = JsonResponse{}

	if Id == "" {
		response = JsonResponse{Type: "error", Message: "You are missing county id parameter."}
	} else {
		db := postgres.Connect()

		fmt.Println("Deleting county")

		_, err := db.Exec(`DELETE FROM al_counties_wgs84 WHERE id = $1`, Id)

		// check errors
		utilities.CheckErr(err)

		response = JsonResponse{Type: "success", Message: fmt.Sprintf("The county has been deleted successfully! Id: %s", Id)}
	}

	json.NewEncoder(w).Encode(response)
}
