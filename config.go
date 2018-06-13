package main

import "github.com/kelseyhightower/envconfig"

type ConfigurationSpec struct {
	Port                    int    `default:"8002"`
	CourseServiceUrl        string `default:"http://127.0.0.1:7310" envconfig:"COURSE_SERVICE_URL"`
	CourseManagerServiceUrl string `default:"http://127.0.0.1:8001" envconfig:"COURSE_MANAGER_SERVICE_URL"`
	DatabaseUrl             string `default:"Geo:aventador10@/CourseProgress" split_words:"true"`
}

var config ConfigurationSpec

func initConfig() {
	envconfig.MustProcess("course_progress", &config)
}
