'''
Default configurations.
'''
#默认的配置文件应该完全符合本地开发环境，这样，无需任何设置，就可以立刻启动服务器。

configs = {
    'debug': True,
    'db': {
        'host': '127.0.0.1',
        'port': 3306,
        'user': 'root',
        'password': '',
        'db': 'awesome'
    },
    'session': {
        'secret': 'Awesome'
    }
}