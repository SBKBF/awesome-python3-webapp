'''
Override configurations.
'''
#用来覆盖某些默认设置
#应用程序读取配置文件需要优先从config_override.py读取。
#把config_default.py作为开发环境的标准配置，把config_override.py作为生产环境的标准配置，
# 我们就可以既方便地在本地开发，又可以随时把应用部署到服务器上。

configs = {
    'db': {
        'host': '127.0.0.1'
    }
}