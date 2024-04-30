package main

import (
	"log"
	"time"

	"github.com/gocql/gocql"
	"github.com/gofiber/fiber/v2"
)

type Server struct {
	App *fiber.App
	DB  Database
}

type Database interface {
	AddNote(body string) error
	GetNotes() ([]*Note, error)
}

type ScyllaStore struct {
	Session *gocql.Session
}

type Note struct {
	Id        gocql.UUID `json:"id"`
	Body      string     `json:"body"`
	CreatedAt time.Time  `json:"created_at"`
}
type NoteReq struct {
	Body string `json:"body"`
}

func (s *Server) RegisterRoutes() {
	s.App.Get("/", func(c *fiber.Ctx) error {
		return c.SendString("Hello world")
	})

	s.App.Post("/note", func(c *fiber.Ctx) error {
		note := new(NoteReq)
		if err := c.BodyParser(note); err != nil {
			return err
		}
		err := s.DB.AddNote(note.Body)
		if err != nil {
			log.Println(err)
			return fiber.ErrInternalServerError
		}
		return c.JSON(fiber.Map{
			"message": "ok",
		})
	})

	s.App.Get("/note", func(c *fiber.Ctx) error {
		notes, err := s.DB.GetNotes()
		if err != nil {
			return c.JSON(fiber.Map{
				"error": err.Error(),
			})
		}
		return c.JSON(notes)
	})
}

func NewServer(db Database) *Server {
	app := fiber.New()
	return &Server{
		App: app,
		DB:  db,
	}
}

func NewScyllaStore() *ScyllaStore {
	cluster := gocql.NewCluster("localhost:9042")
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatal(err)
	}
	return &ScyllaStore{
		Session: session,
	}
}

func (s *ScyllaStore) AddNote(body string) error {
	query := `
  INSERT INTO notes_api.notes (
  id,
  body,
  created_at
  ) VALUES(?, ?, ?)`

	err := s.Session.Query(query, gocql.TimeUUID(), body, time.Now()).Exec()
	return err
}

func (s *ScyllaStore) GetNotes() ([]*Note, error) {
	query := `SELECT * FROM notes_api.notes`
	noteArr := []*Note{}

	scanner := s.Session.Query(query).Iter().Scanner()
	for scanner.Next() {
		note := new(Note)
		err := scanner.Scan(&note.Id, &note.Body, &note.CreatedAt)
		if err != nil {
			return nil, err
		}
		noteArr = append(noteArr, note)
	}
	return noteArr, nil
}

func main() {
	db := NewScyllaStore()

	server := NewServer(db)
	server.RegisterRoutes()
	err := server.App.Listen(":3000")
	if err != nil {
		log.Fatal(err)
	}
}
