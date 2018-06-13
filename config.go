package main

import "github.com/kelseyhightower/envconfig"

type ConfigurationSpec struct {
	Port                    int    `default:"8002"`
	CourseManagerServiceUrl string `default:"http://127.0.0.1:8001" envconfig:"COURSE_MANAGER_SERVICE_URL"`
	DatabaseUrl             string `default:"Geo:aventador10@/CourseProgress"`
}

var config ConfigurationSpec

func initConfig() {
	envconfig.MustProcess("course-progress", &config)
}
