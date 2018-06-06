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
	UserId   string `json:"userId"`
	CourseId string `json:"courseId"`
	TaskId   string `json:"taskId"`
	Progress string `json:"progress"`
}

type ProgressItem struct {
	CourseId string `json:"courseId"`
	TaskId   string `json:"taskId"`
	Progress string `json:"progress"`
}

type TaskProgress struct {
	TaskId   string `json:"taskId"`
	Progress string `json:"progress"`
}

type CourseProgress struct {
	CourseId string         `json:"courseId"`
	Tasks    []TaskProgress `json:"tasks"`
}

type CourseInfo struct {
	Id   string `json:"id"`
	Name string `json:"name"`
	URL  string `json:"url"`
}

func initConnection() {
	var err error
	connection, err = sql.Open("mysql", config.DatabaseUrl)
	if err != nil {
		log.Fatal(err)
	}

	err = connection.Ping()
	if err != nil {
		panic(err.Error())
	} else {
		fmt.Println("Conexiune stabilita")
	}

	var res int
	err = connection.QueryRow("SELECT count(table_name) FROM information_schema.tables WHERE table_schema = 'CourseProgress' AND table_name = 'COURSEPROGRESS'").Scan(&res)
	if err != nil {
		log.Fatal(err)
	}
	if res != 1 {
		stmt, err := connection.Prepare("create table COURSEPROGRESS (user_id varchar(20), course_id varchar(20) NOT NULL, task_id varchar(20) NOT NULL, progress varchar(20) NOT NULL, CONSTRAINT pk_courseprogress PRIMARY KEY (user_id,course_id,task_id))")
		if err != nil {
			log.Fatal(err)
		}
		_, err = stmt.Exec()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("Table COURSEPROGRESS has been successfully created")
	}
}

func closeConnection() {
	connection.Close()
}

func pingConnection() {
	err := connection.Ping()
	if err != nil {
		log.Fatal(err)
	}
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
	if task != "" {
		taskProgress.TaskId = taskID
	}
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

func getUserProgress(userId string) ([]ProgressItem, error) {
	stmt, err := connection.Prepare("select course_id, task_id, progress from COURSEPROGRESS where user_id =?")
	if err != nil {
		fmt.Println("Eroare la select: ", err)
		return nil, err
	}
	defer stmt.Close()
	rows, err := stmt.Query(userId)

	items := make([]ProgressItem, 0)
	for rows.Next() {
		var item ProgressItem
		if err := rows.Scan(&item.CourseId, &item.TaskId, &item.Progress); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	if len(items) == 0 {
		return nil, nil
	}
	return items, nil
}

func getCourseURL(course string) string {
	var courseInfo CourseInfo
	resp, err := http.Get(config.CourseManagerServiceUrl + "/courses/" + course)
	if err != nil {
		log.Fatal(err)
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println(err)
	}
	json.Unmarshal(body, &courseInfo)
	return courseInfo.URL
}

func getAllCoursesURL() map[string]string {
	var coursesInfo []CourseInfo
	URLs := make(map[string]string)
	resp, err := http.Get(config.CourseServiceUrl + "/courses")
	if err != nil {
		log.Fatal(err)
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println(err)
	}
	json.Unmarshal(body, &coursesInfo)
	for _, course := range coursesInfo {
		URLs[course.Id] = course.URL
	}
	return URLs
}

func getCourseTasks(URL string) []string {
	type BaseTaskInfo struct {
		Id    string `json:"id"`
		Title string `json:"title"`
	}

	type TaskGroup struct {
		Title string          `json:"title"`
		Tasks []*BaseTaskInfo `json:"tasks"`
	}

	var taskGroup []TaskGroup
	tasks := make([]string, 0)
	resp, err := http.Get("http://" + URL + "/tasks")
	if err != nil {
		log.Fatal(err)
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println(err)
	}
	json.Unmarshal(body, &taskGroup)

	for _, taskGroup := range taskGroup {
		for _, taskInfo := range taskGroup.Tasks {
			tasks = append(tasks, taskInfo.Id)
		}
	}
	return tasks
}

func getAllTasks(courseTasks []string, seenTasks []TaskProgress) []TaskProgress {
	allTasks := make([]TaskProgress, 0)
	var task TaskProgress
	for _, courseTask := range courseTasks {
		var seen = false
		for _, seenTask := range seenTasks {
			if courseTask == seenTask.TaskId {
				seen = true
				task.TaskId = seenTask.TaskId
				task.Progress = seenTask.Progress
				allTasks = append(allTasks, task)
			}
		}
		if !seen {
			task.TaskId = courseTask
			task.Progress = "not started"
			allTasks = append(allTasks, task)
		}
	}
	return allTasks
}

func getAllUserTasks(courseTasks []string, seenItems []ProgressItem, courseId string) []ProgressItem {
	progressItems := make([]ProgressItem, 0)
	var item ProgressItem
	for _, courseTask := range courseTasks {
		var seen = false
		for _, seenItem := range seenItems {
			if courseTask == seenItem.TaskId {
				seen = true
				item.CourseId = seenItem.CourseId
				item.TaskId = seenItem.TaskId
				item.Progress = seenItem.Progress
				progressItems = append(progressItems, item)
			}
		}
		if !seen {
			item.CourseId = courseId
			item.TaskId = courseTask
			item.Progress = "not started"
			progressItems = append(progressItems, item)
		}
	}
	return progressItems
}

func HandleUserCourseGet(w http.ResponseWriter, _ *http.Request, ps httprouter.Params) {
	pingConnection()

	var URL = getCourseURL(ps.ByName("course"))
	var courseTasks = getCourseTasks(URL)

	if len(courseTasks) != 0 {
		var courseProgress CourseProgress
		courseProgress.CourseId = ps.ByName("course")
		courseProgress.Tasks = getCourseProgress(ps.ByName("user"), ps.ByName("course"))

		if len(courseProgress.Tasks) != 0 {
			var allTasks = getAllTasks(courseTasks, courseProgress.Tasks)
			message, _ := json.Marshal(allTasks)
			w.Header().Set("Content-Type", "application/json")
			w.Write(message)
		} else {
			allTasks := make([]TaskProgress, 0)
			for _, task := range courseTasks {
				var taskProgress TaskProgress
				taskProgress.TaskId = task
				taskProgress.Progress = "not started"
				allTasks = append(allTasks, taskProgress)
				message, _ := json.Marshal(allTasks)
				w.Header().Set("Content-Type", "application/json")
				w.Write(message)
			}
		}
	} else {
		http.Error(w, "User or course not found", http.StatusNotFound)
		return
	}
}

func HandleUserCourseTaskGet(w http.ResponseWriter, _ *http.Request, ps httprouter.Params) {
	pingConnection()

	var URL = getCourseURL(ps.ByName("course"))
	var courseTasks = getCourseTasks(URL)
	if len(courseTasks) != 0 {
		var taskfound = false
		for _, taskId := range courseTasks {
			if taskId == ps.ByName("task") {
				taskfound = true
			}
		}
		if taskfound {
			taskProgress := getTaskProgress(ps.ByName("user"), ps.ByName("course"), ps.ByName("task"))
			if taskProgress.TaskId != "" {
				message, _ := json.Marshal(taskProgress)
				w.Header().Set("Content-Type", "application/json")
				w.Write(message)
			} else {
				var task TaskProgress
				task.TaskId = ps.ByName("task")
				task.Progress = "not started"
				message, _ := json.Marshal(task)
				w.Header().Set("Content-Type", "application/json")
				w.Write(message)
			}
		} else {
			http.Error(w, "Task not found", http.StatusNotFound)
			return
		}
	} else {
		http.Error(w, "User or course not found", http.StatusNotFound)
		return
	}
}

func HandleUserCourseTaskPut(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	pingConnection()

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

func HandleUserGet(w http.ResponseWriter, _ *http.Request, ps httprouter.Params) {
	pingConnection()

	var URLs = getAllCoursesURL()
	userProgress, err := getUserProgress(ps.ByName("user"))
	if err != nil {
		http.Error(w, "Error accesing the database", http.StatusInternalServerError)
		return
	}
	for courseId, URL := range URLs {
		var courseTasks = getCourseTasks(URL)
		progressItems := getAllUserTasks(courseTasks, userProgress, courseId)
		if progressItems != nil {
			message, _ := json.Marshal(progressItems)
			w.Header().Set("Content-Type", "application/json")
			w.Write(message)
		} else {
			http.Error(w, "No progress found", http.StatusNotFound)
			return
		}
	}
}

func main() {
	initConfig()
	initConnection()
	router := httprouter.New()
	router.GET("/:user", HandleUserGet)
	router.GET("/:user/:course", HandleUserCourseGet)
	router.GET("/:user/:course/:task", HandleUserCourseTaskGet)
	router.PUT("/:user/:course/:task", HandleUserCourseTaskPut)
	log.Fatal(http.ListenAndServe(":"+strconv.Itoa(config.Port), router))
	closeConnection()
}
