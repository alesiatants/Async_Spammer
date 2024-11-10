package mymain

import (
	"fmt"
	"sync"
	"sync/atomic"
)

func RunPipeline(cmds ...cmd) {
	var wgroup sync.WaitGroup
	in := make(chan interface{})

	for _, jobFunc := range cmds {
		wgroup.Add(1)
		out := make(chan interface{})
		go workerPipeline(&wgroup, jobFunc, in, out)
		in = out
	}
	wgroup.Wait()
}
func workerPipeline(wg *sync.WaitGroup, jobFunc cmd, in, out chan interface{}) {
	defer wg.Done()
	defer close(out)
	jobFunc(in, out)
}

func SelectUsers(in, out chan interface{}) {
	// 	in - string
	// 	out - User
	seen := make(map[uint64]struct{})
	var wg sync.WaitGroup
	for email := range in {
		wg.Add(1)
		go func(email string) {
			defer wg.Done()
			user := GetUser(email)
			if _, ok := seen[user.ID]; !ok {
				seen[user.ID] = struct{}{}
				out <- user
			}
		}(email.(string))
	}
	wg.Wait()

}

func SelectMessages(in, out chan interface{}) {
	// 	in - User
	// 	out - MsgID
	var users []User
	var wg sync.WaitGroup
	for user := range in {
		users = append(users, user.(User))
		if len(users) == GetMessagesMaxUsersBatch {
			wg.Add(1)
			go func(users []User) {
				defer wg.Done()
				messages, err := GetMessages(users...)
				if err == nil {
					for _, msgID := range messages {
						out <- msgID
					}
				} else {
					atomic.AddUint32(&stat.ErrorGetMessage, 1)
				}
			}(users)
			users = nil
		}
	}

	if len(users) > 0 {
		wg.Add(1)
		go func(users []User) {
			defer wg.Done()
			messages, err := GetMessages(users...)
			if err == nil {
				for _, msgID := range messages {
					out <- msgID
				}
			} else {
				atomic.AddUint32(&stat.ErrorGetMessage, 1)
			}
		}(users)
	}
	wg.Wait()
}

func CheckSpam(in, out chan interface{}) {
	// in - MsgID
	// out - MsgData
	var wg sync.WaitGroup
	var requests int
	for msgID := range in {
		if requests < HasSpamMaxAsyncRequests {
			wg.Add(1)
			go func(id MsgID) {
				defer wg.Done()
				hasSpam, err := HasSpam(id)
				if err == nil {
					out <- MsgData{ID: id, HasSpam: hasSpam}
				} else {
					atomic.AddUint32(&stat.ErrorHasSpam, 1)
				}
			}(msgID.(MsgID))
			requests++
		} else {
			wg.Wait()
			requests = 0
			wg.Add(1)
			go func(id MsgID) {
				defer wg.Done()
				hasSpam, err := HasSpam(id)
				if err == nil {
					out <- MsgData{ID: id, HasSpam: hasSpam}
				} else {
					atomic.AddUint32(&stat.ErrorHasSpam, 1)
				}
			}(msgID.(MsgID))
			requests++
		}
	}
	wg.Wait()
}

func CombineResults(in, out chan interface{}) {
	// in - MsgData
	// out - string
	var msgData []MsgData
	for data := range in {
		msgData = append(msgData, data.(MsgData))
	}
	sortMsgData(msgData)
	for _, data := range msgData {
		out <- fmt.Sprintf("%t %d",data.HasSpam, data.ID)
	}
}

func sortMsgData(msgData []MsgData) {
	for i := 0; i < len(msgData)-1; i++ {
		for j := i + 1; j < len(msgData); j++ {
			if !msgData[i].HasSpam && msgData[j].HasSpam {
				msgData[i], msgData[j] = msgData[j], msgData[i]
			} else if msgData[i].HasSpam == msgData[j].HasSpam && msgData[i].ID > msgData[j].ID { // Сортировка по ID
				msgData[i], msgData[j] = msgData[j], msgData[i]
			}
		}
	}
}
