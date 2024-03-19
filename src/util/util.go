package util

import (
	"fmt"
	"time"
)


func PrintTime() string{
	t1:=time.Now().Year()        //年
	t2:=time.Now().Month()       //月
	t3:=time.Now().Day()         //日
	t4:=time.Now().Hour()        //小时
	t5:=time.Now().Minute()      //分钟
	t6:=time.Now().Second()      //秒
	t7:=time.Now().Nanosecond()/1e6  //ms
	//如果获取UTC时间，则可以使用time.UTC
	return fmt.Sprint(time.Date(t1,t2,t3,t4,t5,t6,t7,time.Local)) //获取当前时间，返回当前时间Time
}
