package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/julienschmidt/httprouter"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
)

var connection *sql.DB

type CourseProgressInfo struct {
	UserId   string `json:"user_id"`
	CourseId string `json:"course_id"`
	TaskId   string `json:"task_id"`
	Progress string `json:"progress"`
}

type TaskProgress struct {
	TaskId   string `json:"task_id"`
	Progress string `json:"progress"`
}

type CourseProgress struct {
	CourseId string         `json:"course_id"`
	Tasks    []TaskProgress `json:"tasks"`
}

func initConnection() {
	var err error
	connection, err = sql.Open("mysql",
		"Geo:password@/CourseProgress")
	if err != nil {
		log.Fatal(err)
	}

	err = connection.Ping()
	if err != nil {
		panic(err.Error())
	} else {
		fmt.Println("Conexiune stabilita")
	}
}

func closeConnection() {
	connection.Close()
}

func getTaskProgress(userID, courseID, taskID string) TaskProgress {
	stmt, err := connection.Prepare("select progress from CourseProgress where User_id = ? and course_id = ? and Task_id = ?")
	if err != nil {
		fmt.Println("Eroare la select: ", err)
		log.Fatal(err)
	}
	defer stmt.Close()
	var task string
	rows, err := stmt.Query(userID, courseID, taskID)
	if rows.Next() {
		err = rows.Scan(&task)
		if err != nil {
			log.Fatal(err)
		}
	}
	var taskProgress TaskProgress
	taskProgress.TaskId = taskID
	taskProgress.Progress = task
	return taskProgress
}

func addTaskProgress(courseProgress CourseProgressInfo) error {
	stmt, err := connection.Prepare("INSERT INTO COURSEPROGRESS(User_id,course_id,Task_id,progress) values (?,?,?,?)")
	if err != nil {
		fmt.Println(err)
		return err
	}
	_, err = stmt.Exec(courseProgress.UserId, courseProgress.CourseId, courseProgress.TaskId, courseProgress.Progress)
	if err != nil {
		fmt.Println(err)
		return err
	}
	return nil
}

func updateTaskProgress(courseProgress CourseProgressInfo) error {
	stmt, err := connection.Prepare("UPDATE COURSEPROGRESS set progress =? where User_id =? and course_id =? and Task_id =?")
	if err != nil {
		fmt.Println(err)
		return err
	}
	_, err = stmt.Exec(courseProgress.Progress, courseProgress.UserId, courseProgress.CourseId, courseProgress.TaskId)
	if err != nil {
		fmt.Println(err)
		return err
	}
	return nil
}

func getCourseProgress(userId, courseId string) []TaskProgress {
	stmt, err := connection.Prepare("select task_id,progress from CourseProgress where User_id = ? and course_id = ?")
	if err != nil {
		fmt.Println("Eroare la select: ", err)
		log.Fatal(err)
	}
	defer stmt.Close()
	rows, err := stmt.Query(userId, courseId)

	var taskId, progress string
	tasks := make([]TaskProgress, 0)
	var newTask TaskProgress

	for rows.Next() {
		err = rows.Scan(&taskId, &progress)
		if err != nil {
			log.Fatal(err)
		}
		newTask.TaskId = taskId
		newTask.Progress = progress
		tasks = append(tasks, newTask)
	}
	return tasks
}

func HandleUserCourseGet(w http.ResponseWriter, _ *http.Request, ps httprouter.Params) {
	var courseProgress CourseProgress
	courseProgress.CourseId = ps.ByName("course")
	courseProgress.Tasks = getCourseProgress(ps.ByName("user"), ps.ByName("course"))
	if len(courseProgress.Tasks) != 0 {
		message, _ := json.Marshal(courseProgress)
		w.Header().Set("Content-Type", "application/json")
		w.Write(message)
	} else {
		http.Error(w, "User didn't start this course", http.StatusNotFound)
	}
}

func HandleUserCourseTaskGet(w http.ResponseWriter, _ *http.Request, ps httprouter.Params) {
	taskProgress := getTaskProgress(ps.ByName("user"), ps.ByName("course"), ps.ByName("task"))
	if taskProgress.Progress != "" {
		message, _ := json.Marshal(taskProgress)
		w.Header().Set("Content-Type", "application/json")
		w.Write(message)
	} else {
		http.Error(w, "The user didn't reach this task", http.StatusNotFound)
	}
}

func HandleUserCourseTaskPut(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		fmt.Println(err)
	}
	var courseProgress CourseProgressInfo
	type ProgressInfo struct {
		Progress string `json:"progress"`
	}
	var progress ProgressInfo
	json.Unmarshal(body, &progress)

	courseProgress.UserId = ps.ByName("user")
	courseProgress.CourseId = ps.ByName("course")
	courseProgress.TaskId = ps.ByName("task")
	courseProgress.Progress = progress.Progress

	if getTaskProgress(courseProgress.UserId, courseProgress.CourseId, courseProgress.TaskId).Progress != "" {
		err = updateTaskProgress(courseProgress)
	} else {
		err = addTaskProgress(courseProgress)
	}

	if err != nil {
		http.Error(w, "Failed to update task progress", http.StatusInternalServerError)
		return
	}
}

func main() {

	initConnection()
	router := httprouter.New()
	router.GET("/:user/:course", HandleUserCourseGet)
	router.GET("/:user/:course/:task", HandleUserCourseTaskGet)
	router.PUT("/:user/:course/:task", HandleUserCourseTaskPut)
	log.Fatal(http.ListenAndServe(":"+strconv.Itoa(82), router))
	closeConnection()
}
