package drive
import(
	"fmt"
	rd "github.com/go-redis/redis"
)

var	redis *rd.Client

func OpenRedis(host string, port int, password string) error{
	redis = rd.NewClient(&rd.Options{
		Addr:     fmt.Sprintf("%s:%d", host, port),
		Password: password, // no password set
		DB:       0,                     // use default DB
	})
	//测试Redis是否连接成功
	_, err := redis.Ping().Result()
	if err != nil {
		return err
	}
	return nil
}

//GetRedis获得Redis句柄
func GetRedis() *rd.Client {
	return redis
}
