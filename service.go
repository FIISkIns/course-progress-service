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

//Initializes the connection with database
//Creates COURSEPROGRESS table in database if it not exist
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
		log.Println("Creating table COURSEPROGRESS" + err.Error())
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

//Get progress from database for the specified user,course and task
//Returns the task progress and the error
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

//Inserts in database new task progress
//Returns nil on success or error otherwise
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

//Update  in database the task progress
//Returns nil on success or error otherwise
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

//Get progress from database for the specified user and course
//Returns all tasks with progress and the error
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

//Get progress from database for the specified user
//Returns all courses with tasks and progress, and the error
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

//Get the course-service URL for the specified course from course-manager-service
//Returns the URL and nil on success or empty string and nil
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

//Get all course-services URLs from course-manager-service
//Returns a map of course ids and URLs and nil on success or empty map and nil
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

//Get all tasks from the course at the specified URL
//Returns a slice of tasks and nil on success or empty slice and error
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

//Get all available tasks with progress from the given list of available tasks and the task progress stored on database
//If the task isn't found in the second list, the default value of progress will be 'not started'
//Returns a list of TaskProgress
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

//Get all available tasks with progress from the given list of available tasks and the task progress stored on database
//If the task isn't found in the second list, the default value of progress will be 'not started' and the default value of course will be the specified courseId
//Returns a list of ProgressItem
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

//Handles the get method on /progress/:user/:course
//It get the available tasks from the course-service and the progress stored on database
//Returns 200 status code and the course progress on success or the error cause with the proper error code
func HandleUserCourseGet(w http.ResponseWriter, _ *http.Request, ps httprouter.Params) {
	err := connection.Ping()
	if err != nil {
		errorMessage := "Database error: unable to connect. \nCause: " + err.Error()
		log.Println(errorMessage)
		http.Error(w, errorMessage, http.StatusInternalServerError)
		return
	}

	var URL string
	URL, err = getCourseURL(ps.ByName("course"))
	if err != nil {
		errorMessage := "Server error: Request to course-manager-service failed. Can not get course URL. \nCause: " + err.Error()
		log.Println(errorMessage)
		http.Error(w, errorMessage, http.StatusInternalServerError)
		return
	}

	var courseTasks []string
	courseTasks, err = getCourseTasks(URL)
	if err != nil {
		errorMessage := "Server error: Request to course-service failed. Can not retrieve tasks. \nCause: " + err.Error()
		log.Println(errorMessage)
		http.Error(w, errorMessage, http.StatusInternalServerError)
		return
	}

	if len(courseTasks) != 0 {
		var courseProgress CourseProgress
		courseProgress.CourseId = ps.ByName("course")
		courseProgress.Tasks, err = getCourseProgress(ps.ByName("user"), ps.ByName("course"))
		if err != nil {
			errorMessage := "Database error: can not get course progress. \nCause: " + err.Error()
			log.Println(errorMessage)
			http.Error(w, errorMessage, http.StatusInternalServerError)
			return
		}

		if len(courseProgress.Tasks) != 0 {
			var allTasks = getAllTasks(courseTasks, courseProgress.Tasks)
			message, err := json.Marshal(allTasks)
			if err != nil {
				errorMessage := "JSON error: failed to marshall progress. \nCause: " + err.Error()
				log.Println(errorMessage)
				http.Error(w, errorMessage, http.StatusInternalServerError)
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
				errorMessage := "JSON error: failed to marshall progress. \nCause: " + err.Error()
				log.Println(errorMessage)
				http.Error(w, errorMessage, http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.Write(message)
		}
	} else {
		errorMessage := "User or course not found. Course service at " + URL + " return no tasks"
		log.Println(errorMessage)
		http.Error(w, errorMessage, http.StatusNotFound)
		return
	}
}

//Handles the get method on /progress/:user/:course/:task
//It get the available tasks from the course-service and the progress stored on database
//Returns 200 status code and the task progress on success or the error cause with the proper error code
func HandleUserCourseTaskGet(w http.ResponseWriter, _ *http.Request, ps httprouter.Params) {
	err := connection.Ping()
	if err != nil {
		errorMessage := "Database error: unable to connect. \nCause: " + err.Error()
		log.Println(errorMessage)
		http.Error(w, errorMessage, http.StatusInternalServerError)
	}

	var URL string
	URL, err = getCourseURL(ps.ByName("course"))
	if err != nil {
		errorMessage := "Server error: Request to course-manager-service failed. Can not get course URL. \nCause: " + err.Error()
		log.Println(errorMessage)
		http.Error(w, errorMessage, http.StatusInternalServerError)
		return
	}

	var courseTasks []string
	courseTasks, err = getCourseTasks(URL)
	if err != nil {
		errorMessage := "Server error: Request to course-service failed. Can not retrieve tasks. \nCause: " + err.Error()
		log.Println(errorMessage)
		http.Error(w, errorMessage, http.StatusInternalServerError)
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
				errorMessage := "Database error: can not get task's progress. \nCause: " + err.Error()
				log.Println(errorMessage)
				http.Error(w, errorMessage, http.StatusInternalServerError)
				return
			}
			if taskProgress.TaskId != "" {
				message, err := json.Marshal(taskProgress)
				if err != nil {
					errorMessage := "JSON error: failed to marshall progress. \nCause: " + err.Error()
					log.Println(errorMessage)
					http.Error(w, errorMessage, http.StatusInternalServerError)
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
					errorMessage := "JSON error: failed to marshall progress. \nCause: " + err.Error()
					log.Println(errorMessage)
					http.Error(w, errorMessage, http.StatusInternalServerError)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				w.Write(message)
			}
		} else {
			errorMessage := "Task + " + ps.ByName("task") + " not found"
			log.Println(errorMessage)
			http.Error(w, errorMessage, http.StatusNotFound)
			return
		}
	} else {
		errorMessage := "User or course not found. Course service at " + URL + " return no tasks"
		log.Println(errorMessage)
		http.Error(w, errorMessage, http.StatusNotFound)
		return
	}
}

//Handles the put method on /progress/:user/:course/:task
//It updates or insert the progress of the given user, course and task
//Returns 200 status code and the course progress on success or the error cause with the proper error code
func HandleUserCourseTaskPut(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	err := connection.Ping()
	if err != nil {
		errorMessage := "Database error: unable to connect. \nCause: " + err.Error()
		log.Println(errorMessage)
		http.Error(w, errorMessage, http.StatusInternalServerError)
		return
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		errorMessage := "Failed to read HTTP body. \nCause: " + err.Error()
		log.Println(errorMessage)
		http.Error(w, errorMessage, http.StatusBadRequest)
		return
	}

	var courseProgress CourseProgressInfo
	type ProgressInfo struct {
		Progress string `json:"progress"`
	}
	var progress ProgressInfo
	err = json.Unmarshal(body, &progress)
	if err != nil {
		errorMessage := "Failed to unmarshal request. \nCause: " + err.Error()
		log.Println(errorMessage)
		http.Error(w, errorMessage, http.StatusInternalServerError)
		return
	}

	if progress.Progress != "started" && progress.Progress != "completed" {
		errorMessage := "Invalid progress type. Valid types: 'started','completed'"
		log.Println(errorMessage)
		http.Error(w, errorMessage, http.StatusUnprocessableEntity)
		return
	}

	courseProgress.UserId = ps.ByName("user")
	courseProgress.CourseId = ps.ByName("course")
	courseProgress.TaskId = ps.ByName("task")
	courseProgress.Progress = progress.Progress

	taskExist, err := getTaskProgress(courseProgress.UserId, courseProgress.CourseId, courseProgress.TaskId)
	if err != nil {
		errorMessage := "Database error. \nCause: " + err.Error()
		log.Println(errorMessage)
		http.Error(w, errorMessage, http.StatusInternalServerError)
		return
	}

	if taskExist.Progress != "" {
		err = updateTaskProgress(courseProgress)
	} else {
		err = addTaskProgress(courseProgress)
	}

	if err != nil {
		errorMessage := "Failed to update task progress. \nCause: " + err.Error()
		log.Println(errorMessage)
		http.Error(w, errorMessage, http.StatusInternalServerError)
		return
	}
}

//Handles the get method on /progress/:user
//It get the available courses from the course-service and the progress stored on database
//Returns 200 status code and the user progress on success or the error cause with the proper error code
func HandleUserGet(w http.ResponseWriter, _ *http.Request, ps httprouter.Params) {
	err := connection.Ping()
	if err != nil {
		errorMessage := "Database error: unable to connect. \nCause: " + err.Error()
		log.Println(errorMessage)
		http.Error(w, errorMessage, http.StatusInternalServerError)
		return
	}

	var URLs map[string]string
	URLs, err = getAllCoursesURL()
	if err != nil {
		errorMessage := "Server error: Request to course-manager-service failed. Can not retrieve all courses's URLs. \nCause: " + err.Error()
		log.Println(errorMessage)
		http.Error(w, errorMessage, http.StatusInternalServerError)
		return
	} else if URLs == nil {
		errorMessage := "No URLs found. Course-manager-service returns no URL"
		log.Println(errorMessage)
		http.Error(w, errorMessage, http.StatusNotFound)
		return
	}

	userProgress, err := getUserProgress(ps.ByName("user"))
	if err != nil {
		errorMessage := "Error accessing the database. \nCause: " + err.Error()
		log.Println(errorMessage)
		http.Error(w, errorMessage, http.StatusInternalServerError)
		return
	}

	var emptyResponse = true
	allProgressItems := make([]ProgressItem, 0)

	for courseId, URL := range URLs {
		var courseTasks []string
		courseTasks, err = getCourseTasks(URL)
		if err != nil {
			errorMessage := "Server error: Request to course-service failed. Can not retrieve tasks. \nCause: " + err.Error()
			log.Println(errorMessage)
			http.Error(w, errorMessage, http.StatusInternalServerError)
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
		errorMessage := "No courses information found. No progress found"
		log.Println(errorMessage)
		http.Error(w, errorMessage, http.StatusNotFound)
		return
	}
	if allProgressItems != nil {
		message, err := json.Marshal(allProgressItems)
		if err != nil {
			errorMessage := "JSON error: failed to marshall progress" + err.Error()
			log.Println(errorMessage)
			http.Error(w, errorMessage, http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(message)
	}
}

//Checks if the service at the given URL is UP
//Returns true if the service is UP or false otherwise
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

//Handle the get and head method on /health
//Verify the connection with the database and the status of dependent services
func HandleHealthCheck(w http.ResponseWriter, _ *http.Request, _ httprouter.Params) {
	if err := connection.Ping(); err != nil {
		errorMessage := "Database connection failed: " + err.Error()
		log.Println(errorMessage)
		http.Error(w, errorMessage, http.StatusInternalServerError)
		return
	}
	if success := checkHealth(w, config.CourseManagerServiceUrl+"/health"); !success {
		return
	}
	URLs, err := getAllCoursesURL()
	if err != nil {
		errorMessage := "Failed to get course services from course-manager-service \nCause: " + err.Error()
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
