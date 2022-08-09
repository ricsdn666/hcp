from datetime import datetime

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.redis import RedisJobStore
from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.executors.pool import ProcessPoolExecutor




REDIS_DB = {
    "db": 1,
    "host": "127.0.0.1"
}

def func(name):
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(now + f" Hello world, {name}")

interval_task = {
    # 配置存储器
    '''
    "jobstores": {
        'default': RedisJobStore(**REDIS_DB)
    },
    '''
    # 配置执行器
	"executors": {
	    # 使用进程池进行调度，最大进程数是10个
	    'default': ProcessPoolExecutor(10)
	},
    # 创建job时的默认参数
    "job_defaults": {
        'coalesce': False,  # 是否合并执行
        'max_instances': 3,  # 最大实例数
    }

}
scheduler = AsyncIOScheduler(**interval_task)
# 添加一个定时任务
#scheduler.add_job(func, 'interval', seconds=3, args=["desire"], id="desire_job", replace_existing=True)
#scheduler.add_job(out.fee2patient(), 'interval', seconds=3, args=["desire"], id="desire_job", replace_existing=True)