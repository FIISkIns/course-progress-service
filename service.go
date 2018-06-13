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
		log.Fatal("Failed to connect to database")
	}

	err = connection.Ping()
	if err != nil {
		log.Fatal("Failed to connect to database" + err.Error())
	} else {
		log.Println("Conexiune stabilita")
	}

	var res string
	err = connection.QueryRow("SELECT user_id FROM COURSEPROGRESS LIMIT 1").Scan(&res)
	if err != nil && err != sql.ErrNoRows {
		log.Println("Creating table COURSEPROGRESS"+err.Error())
		stmt, err := connection.Prepare("create table COURSEPROGRESS (" +
			" user_id varchar(100) NOT NULL," +
			" course_id varchar(100) NOT NULL," +
			" task_id varchar(100) NOT NULL," +
			" progress varchar(30) NOT NULL," +
			" CONSTRAINT pk_courseprogress PRIMARY KEY (user_id,course_id,task_id))")
		if err != nil {
			log.Fatal(err)
		}
		_, err = stmt.Exec()
		if err != nil {
			log.Fatal(err)
		}
		log.Println("Table COURSEPROGRESS has been successfully created")
	}
}

func getTaskProgress(userID, courseID, taskID string) (*TaskProgress, error) {
	stmt, err := connection.Prepare("select progress from COURSEPROGRESS where user_id = ? and course_id = ? and task_id = ?")
	if err != nil {
		fmt.Println("Eroare la select: ", err)
		return nil, err
	}
	defer stmt.Close()
	var task string
	rows, err := stmt.Query(userID, courseID, taskID)
	if err != nil {
		return nil, err
	}
	if rows.Next() {
		err = rows.Scan(&task)
		if err != nil {
			return nil, err
		}
	}
	var taskProgress TaskProgress
	if task != "" {
		taskProgress.TaskId = taskID
	}
	taskProgress.Progress = task
	return &taskProgress, nil
}

func addTaskProgress(courseProgress CourseProgressInfo) error {
	stmt, err := connection.Prepare("INSERT INTO COURSEPROGRESS(user_id,course_id,task_id,progress) values (?,?,?,?)")
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

func getCourseProgress(userId, courseId string) ([]TaskProgress, error) {
	stmt, err := connection.Prepare("select task_id,progress from COURSEPROGRESS where user_id = ? and course_id = ?")
	if err != nil {
		fmt.Println("Eroare la select: ", err)
		return nil, err
	}
	defer stmt.Close()
	rows, err := stmt.Query(userId, courseId)
	if err != nil {
		return nil, err
	}

	var taskId, progress string
	tasks := make([]TaskProgress, 0)
	var newTask TaskProgress

	for rows.Next() {
		err = rows.Scan(&taskId, &progress)
		if err != nil {
			fmt.Println("Database error during iterating tasks")
			return nil, err
		}
		newTask.TaskId = taskId
		newTask.Progress = progress
		tasks = append(tasks, newTask)
	}
	return tasks, nil
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

func getCourseURL(course string) (string, error) {
	var courseInfo CourseInfo
	resp, err := http.Get(config.CourseManagerServiceUrl + "/courses/" + course)
	if err != nil {
		fmt.Println("Server error: Request to course-manager-service failed. Can not get course URL " + err.Error())
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		fmt.Println("Course-manager-service returned error code ", resp.StatusCode)
		return "", err
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("Failed to read body response")
		return "", err
	}
	err = json.Unmarshal(body, &courseInfo)
	if err != nil {
		fmt.Println("Failed to unmarshal body resoponse" + err.Error())
		return "", err
	}
	return courseInfo.URL, err
}

func getAllCoursesURL() (map[string]string, error) {
	var coursesInfo []CourseInfo
	URLs := make(map[string]string)
	resp, err := http.Get(config.CourseManagerServiceUrl + "/courses")
	if err != nil {
		fmt.Println("Server error: Request to course-manager-service failed. Can not get all course's URLs" + config.CourseManagerServiceUrl)
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		fmt.Println("Course-manager-service returned error code ", resp.StatusCode)
		return nil, err
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("Failed to read body response")
		return nil, err
	}
	err = json.Unmarshal(body, &coursesInfo)
	if err != nil {
		fmt.Println("Failed to unmarshal body resoponse" + err.Error())
		return nil, err
	}

	for _, course := range coursesInfo {
		URLs[course.Id] = course.URL
	}
	return URLs, nil
}

func getCourseTasks(URL string) ([]string, error) {
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
	resp, err := http.Get(URL + "/tasks")
	if err != nil {
		fmt.Println("Server error: Request to course-service failed. Can not retrieve tasks from " + URL + "/tasks")
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		fmt.Println("Course-service returned error code ", resp.StatusCode)
		return nil, err
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("Failed to read body response")
		return nil, err
	}
	err = json.Unmarshal(body, &taskGroup)
	if err != nil {
		fmt.Println("Failed to unmarshal body resoponse")
		return nil, err
	}

	for _, taskGroup := range taskGroup {
		for _, taskInfo := range taskGroup.Tasks {
			tasks = append(tasks, taskInfo.Id)
		}
	}
	return tasks, nil
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
	err := connection.Ping()
	if err != nil {
		http.Error(w, "Database error: unable to connect", http.StatusInternalServerError)
		return
	}

	var URL string
	URL, err = getCourseURL(ps.ByName("course"))
	if err != nil {
		http.Error(w, "Server error: Request to course-manager-service failed. Can not get course URL", http.StatusInternalServerError)
		return
	}

	var courseTasks []string
	courseTasks, err = getCourseTasks(URL)
	if err != nil {
		http.Error(w, "Server error: Request to course-service failed. Can not retrieve tasks", http.StatusInternalServerError)
		return
	}

	if len(courseTasks) != 0 {
		var courseProgress CourseProgress
		courseProgress.CourseId = ps.ByName("course")
		courseProgress.Tasks, err = getCourseProgress(ps.ByName("user"), ps.ByName("course"))
		if err != nil {
			http.Error(w, "Database error: can not get course progress", http.StatusInternalServerError)
			return
		}

		if len(courseProgress.Tasks) != 0 {
			var allTasks = getAllTasks(courseTasks, courseProgress.Tasks)
			message, err := json.Marshal(allTasks)
			if err != nil {
				http.Error(w, "JSON error: failed to marshall progress", http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.Write(message)
		} else {
			allTasks := make([]TaskProgress, 0)
			for _, task := range courseTasks {
				var taskProgress TaskProgress
				taskProgress.TaskId = task
				taskProgress.Progress = "not started"
				allTasks = append(allTasks, taskProgress)
			}
			message, err := json.Marshal(allTasks)
			if err != nil {
				http.Error(w, "JSON error: failed to marshall progress", http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.Write(message)
		}
	} else {
		http.Error(w, "User or course not found", http.StatusNotFound)
		return
	}
}

func HandleUserCourseTaskGet(w http.ResponseWriter, _ *http.Request, ps httprouter.Params) {
	err := connection.Ping()
	if err != nil {
		http.Error(w, "Database error: unable to connect", http.StatusInternalServerError)
	}

	var URL string
	URL, err = getCourseURL(ps.ByName("course"))
	if err != nil {
		http.Error(w, "Server error: Request to course-manager-service failed. Can not get course URL", http.StatusInternalServerError)
		return
	}

	var courseTasks []string
	courseTasks, err = getCourseTasks(URL)
	if err != nil {
		http.Error(w, "Server error: Request to course-service failed. Can not retrieve tasks", http.StatusInternalServerError)
		return
	}

	if len(courseTasks) != 0 {
		var taskFound = false
		for _, taskId := range courseTasks {
			if taskId == ps.ByName("task") {
				taskFound = true
			}
		}
		if taskFound {
			taskProgress, err := getTaskProgress(ps.ByName("user"), ps.ByName("course"), ps.ByName("task"))
			if err != nil {
				http.Error(w, "Database error: can not get task's progress", http.StatusInternalServerError)
				return
			}
			if taskProgress.TaskId != "" {
				message, err := json.Marshal(taskProgress)
				if err != nil {
					http.Error(w, "JSON error: failed to marshall progress", http.StatusInternalServerError)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				w.Write(message)
			} else {
				var task TaskProgress
				task.TaskId = ps.ByName("task")
				task.Progress = "not started"
				message, err := json.Marshal(task)
				if err != nil {
					http.Error(w, "JSON error: failed to marshall progress", http.StatusInternalServerError)
					return
				}
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
	err := connection.Ping()
	if err != nil {
		http.Error(w, "Database error: unable to connect", http.StatusInternalServerError)
		return
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read HTTP body", http.StatusBadRequest)
		return
	}

	var courseProgress CourseProgressInfo
	type ProgressInfo struct {
		Progress string `json:"progress"`
	}
	var progress ProgressInfo
	err = json.Unmarshal(body, &progress)
	if err != nil {
		http.Error(w, "Failed to unmarshal request", http.StatusInternalServerError)
		return
	}

	if progress.Progress != "started" && progress.Progress != "completed" {
		http.Error(w, "Invalid progress type. Valid types: 'started','completed'", http.StatusUnprocessableEntity)
		return
	}

	courseProgress.UserId = ps.ByName("user")
	courseProgress.CourseId = ps.ByName("course")
	courseProgress.TaskId = ps.ByName("task")
	courseProgress.Progress = progress.Progress

	taskExist, err := getTaskProgress(courseProgress.UserId, courseProgress.CourseId, courseProgress.TaskId)
	if err != nil {
		http.Error(w, "Database error", http.StatusInternalServerError)
		return
	}

	if taskExist.Progress != "" {
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
	err := connection.Ping()
	if err != nil {
		http.Error(w, "Database error: unable to connect", http.StatusInternalServerError)
		return
	}

	var URLs map[string]string
	URLs, err = getAllCoursesURL()
	if err != nil {
		http.Error(w, "Server error: Request to course-manager-service failed. Can not retrieve all courses's URLs", http.StatusInternalServerError)
		return
	} else if URLs == nil {
		http.Error(w, "No URLs found", http.StatusNotFound)
		return
	}

	userProgress, err := getUserProgress(ps.ByName("user"))
	if err != nil {
		http.Error(w, "Error accessing the database", http.StatusInternalServerError)
		return
	}

	var emptyResponse = true
	allProgressItems := make([]ProgressItem, 0)

	for courseId, URL := range URLs {
		var courseTasks []string
		courseTasks, err = getCourseTasks(URL)
		if err != nil {
			http.Error(w, "Server error: Request to course-service failed. Can not retrieve tasks", http.StatusInternalServerError)
			return
		}

		if courseTasks != nil {
			emptyResponse = false
			progressItems := getAllUserTasks(courseTasks, userProgress, courseId)
			if progressItems != nil {
				allProgressItems = append(allProgressItems, progressItems...)
			}
		}
	}
	if emptyResponse {
		http.Error(w, "No courses information found. No progress found", http.StatusNotFound)
		return
	}
	if allProgressItems != nil {
		message, err := json.Marshal(allProgressItems)
		if err != nil {
			http.Error(w, "JSON error: failed to marshall progress", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(message)
	}
}

func checkHealth(w http.ResponseWriter, url string) bool {
	resp, err := http.Get(url)
	if err != nil {
		errorMessage := "Failed to communicate with: " + url + "\nCause: " + err.Error()
		log.Println(errorMessage)
		http.Error(w, errorMessage, http.StatusInternalServerError)
		return false
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		errorMessage := "Failed to read response from: " + url + "\nCause: " + err.Error()
		log.Println(errorMessage)
		http.Error(w, errorMessage, http.StatusInternalServerError)
		return false
	}
	if (resp.StatusCode / 100) != 2 {
		errorMessage := "Failed health check on: " + url + "\nResponse: " + string(body)
		log.Println(errorMessage)
		http.Error(w, errorMessage, http.StatusInternalServerError)
		return false
	}
	return true
}

func HandleHealthCheck(w http.ResponseWriter, _ *http.Request, _ httprouter.Params) {
	if err := connection.Ping(); err != nil {
		errorMessage := "Database connection failed: "+ err.Error()
		log.Println(errorMessage)
		http.Error(w, errorMessage, http.StatusInternalServerError)
		return
	}
	if success := checkHealth(w, config.CourseManagerServiceUrl+"/health"); !success {
		return
	}
	URLs, err := getAllCoursesURL()
	if err != nil {
		errorMessage := "Failed to get course services from course-manager-service \nCause: "+err.Error()
		log.Println(errorMessage)
		http.Error(w, errorMessage, http.StatusInternalServerError)
		return
	}
	for _, URL := range URLs {
		if success := checkHealth(w, URL+"/health"); !success {
			return
		}
	}
}

func main() {
	initConfig()
	initConnection()
	defer connection.Close()
	router := httprouter.New()
	router.GET("/progress/:user", HandleUserGet)
	router.GET("/progress/:user/:course", HandleUserCourseGet)
	router.GET("/progress/:user/:course/:task", HandleUserCourseTaskGet)
	router.PUT("/progress/:user/:course/:task", HandleUserCourseTaskPut)
	router.GET("/health", HandleHealthCheck)
	router.HEAD("/health", HandleHealthCheck)
	log.Fatal(http.ListenAndServe(":"+strconv.Itoa(config.Port), router))
}
