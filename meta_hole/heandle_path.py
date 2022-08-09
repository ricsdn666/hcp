# handle_path
import os

# 1-工程路径
# os.path.abspath(__file__)   当前文件路径
# os.path.dirname()  #返回文件所在的目录
# project_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
project_path = os.path.dirname(__file__)
# project_path2 = os.path.dirname(os.path.abspath(__file__))

# 2- 截图路径
# os.path.join(参数1,参数2)  拼接参数1和参数2的路径
screenshot_path = os.path.join(project_path, r'outFiles\screenshot')

# 3- 日志路径
logs_path = os.path.join(project_path, r'outFiles\logs')

# 4- 测试数据路径
testData_path = os.path.join(project_path, 'ui_data')

# 5- 测试报告路径
reports_path = os.path.join(project_path, r'outFiles\reports\tmp')

# 6- 配置路径
config_path = os.path.join(project_path, 'configs')
if __name__ == '__main__':
    print(project_path)
    print(config_path)
