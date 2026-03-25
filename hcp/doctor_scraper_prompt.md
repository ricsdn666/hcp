# 医院医生信息抓取提示词

对 __HOSPITAL_NAME__ 官网进行抓取，提取该医院所有可见医生的详细信息，经过结构化字段提取和数据核验后，将结果逐条实时推送到 RabbitMQ 队列。

---

## 一、执行步骤（按顺序执行）

### 1. 探索官网结构

**页面类型识别：**
- 先通过搜索引擎找到官网正确地址
- 分析网站导航，找到"专家介绍"、"医生团队"、"专家团队"、"科室医生"等入口
- **识别医生列表页的布局类型**（重要）：
  - 表格布局：医生信息以行列表格展示
  - 卡片布局：每个医生独立卡片形式
  - 混合布局：上述两种的结合
- 识别医生列表页和详情页的URL模式
- 确认是否存在分页机制（传统分页/滚动加载/点击加载更多）
- **识别反爬虫机制**：验证码、访问频率限制、Cookie验证等

**DOM结构分析：**
- 打开浏览器开发者工具，查看页面HTML结构
- 记录医生列表的容器元素（class/id）
- 记录各字段的选择器路径
- 检查是否存在动态加载内容（AJAX/JavaScript渲染）

### 2. 抓取医生列表

**根据布局类型选择提取策略：**

**表格布局提取：**
- 定位表格容器：`table.doctor-list-table` 或类似
- 遍历每行：`tbody/tr`
- 提取字段：姓名（第1列）、科室（第2列）、职称（第3列）等
- 收集详情链接：通常在姓名列的`<a>`标签

**卡片布局提取：**
- 定位卡片容器：`div.doctor-list` 或 `div.doctor-cards`
- 遍历每个卡片：`div.doctor-card`
- 提取字段：从卡片的各个子元素提取
- 收集详情链接：通常在卡片底部或标题

**分页处理：**
- 传统分页：点击页码按钮，观察URL变化规律
- 滚动加载：滚动到底部，监控网络请求中的数据接口
- 点击加载：点击"加载更多"按钮，分析请求参数

**遍历策略：**
- 遍历所有科室/专业分类，确保不遗漏
- 记录每个科室的医生数量，用于验证完整性
- 收集所有医生详情页URL，去重后保存

### 3. 抓取医生详情

**信息区块识别：**
医生详情页通常包含以下区块，需要逐个识别和提取：

1. **基础信息区**（顶部或左侧）：
   - 医生姓名（h1/h2标题）
   - 职称（标签或介绍文本）
   - 所属科室（链接或文本）
   - 行政职务（如"科室主任"）
   - 医生照片

2. **专业特长区**：
   - 擅长疾病列表
   - 诊疗范围描述
   - 特色技术

3. **个人简介区**：
   - 教育背景（毕业院校、学历、学位）
   - 工作经历
   - 从业年限
   - 学术任职
   - 科研成果

4. **出诊信息区**：
   - 门诊时间表
   - 号源情况

**字段提取规则：**

**必须准确提取的字段：**
- 医生姓名：2-4个汉字，从标题或首行提取
- 科室：展示科室名，几乎所有的医生详情页都有
- 简介：完整的个人介绍原文

**结构化字段提取：**
从简介和擅长领域中提取以下字段，使用正则表达式匹配：

| 字段 | 提取模式 | 示例 |
|------|---------|------|
| 职称 | 匹配关键词：主任医师、副主任医师、主治医师、住院医师 | "主任医师" |
| 行政职务 | 匹配：XX科主任、XX科副主任、副院长、院长等 | "心内科主任" |
| 学术头衔 | 匹配：教授、副教授、博士生导师、硕士生导师等，多个用顿号连接 | "教授、博士生导师" |
| 学历 | 按优先级匹配：博士研究生 > 硕士研究生 > 本科 > 大专 | "博士研究生" |
| 学位 | 匹配：博士 > 硕士 > 学士 | "博士" |
| 毕业院校 | 提取模式：毕业于XX大学、XX医学院、XX大学XX博士 | "北京医科大学" |
| 从业年限 | 提取模式：从事XX工作N年、从医N年、从业XX余年 | 28 |

**正则表达式示例：**
```
从业年限：从事.*?(\d+).*?年
学历：(博士|硕士|本科|大专)研究生?
毕业院校：毕业于(.{2,10}大学|.{2,10}医学院)
行政职务：(.{2,10}主任|.{2,10}院长|副院长)
```

**动态内容处理：**
- 如果页面内容很少，可能使用了动态加载
- 检查网络请求，查找API数据接口
- 使用Playwright等待元素加载完成
- 滚动页面触发懒加载内容
- 点击展开按钮显示隐藏信息

### 4. 数据核验与推送

**多级质量检查：**

**L1 格式验证（必须通过）：**
1. 姓名：2-4个汉字，且不包含"一直致力、教授、硕士、博士、主任医师、副主任医师、友情链接"等词汇
2. URL：必须是有效的医生详情页URL
3. 必填字段不为空

**L2 内容验证（重要）：**
1. 医院名称：必须包含目标医院名称
2. 科室名称：必须与医院官网的科室名一致
3. 职称：必须在标准职称列表中

**L3 逻辑验证（建议）：**
1. 从业年限与年龄合理性（18-70岁范围）
2. 学历与职称匹配度
3. 简介内容与姓名一致性

**核验失败的处理：**
- L1失败：记录失败原因，不推送到队列
- L2失败：记录失败原因和URL，重新理解详情页内容，重新提取后再推送
- L3失败：标记警告但仍推送，降低confidence_score

**推送流程：**
1. 验证数据合法性
2. 推送到 RabbitMQ 队列
3. 本地备份（防止推送失败）
4. 记录推送日志

---

## 二、网页内容提取策略

### 1. 页面类型识别与选择器策略

**表格布局特征：**

典型HTML结构：
```html
<table class="doctor-list-table">
    <thead>
        <tr>
            <th>医生姓名</th>
            <th>科室</th>
            <th>职称</th>
            <th>专业特长</th>
        </tr>
    </thead>
    <tbody>
        <tr class="doctor-row">
            <td class="doctor-name"><a href="/doctor/1001">张华</a></td>
            <td class="department">心血管内科</td>
            <td class="title">主任医师</td>
            <td class="specialty">冠心病、高血压</td>
        </tr>
    </tbody>
</table>
```

XPath选择器：
```
基础路径：//table[contains(@class, 'doctor-list')]//tbody/tr
姓名：./td[contains(@class, 'name')]//a/text()
科室：./td[contains(@class, 'department')]/text()
职称：./td[contains(@class, 'title')]/text()
详情链接：./td[contains(@class, 'name')]//a/@href
```

**卡片布局特征：**

典型HTML结构：
```html
<div class="doctor-list">
    <div class="doctor-card">
        <div class="card-header">
            <img class="avatar" src="/images/doctor/2001.jpg"/>
            <div class="basic-info">
                <h3 class="name">王建国</h3>
                <span class="title">主任医师</span>
            </div>
        </div>
        <div class="card-body">
            <div class="dept-value">骨科</div>
            <div class="specialty-value">颈椎病、腰椎间盘突出</div>
        </div>
        <div class="card-footer">
            <a href="/doctor/detail/2001" class="btn-detail">查看详情</a>
        </div>
    </div>
</div>
```

XPath选择器：
```
基础路径：//div[contains(@class, 'doctor-list')]//div[contains(@class, 'doctor-card')]
姓名：./div[contains(@class, 'header')]//h3/text()
职称：./div[contains(@class, 'header')]//span[contains(@class, 'title')]/text()
科室：./div[contains(@class, 'body')]//div[contains(@class, 'dept')]/text()
详情链接：./div[contains(@class, 'footer')]//a/@href
```

**通用选择器（兜底方案）：**
```
姓名：//h1/text() | //h2[contains(@class, 'name')]/text()
科室：//div[contains(@class, 'department')]/text() | //a[contains(@class, 'dept')]/text()
职称：//span[contains(@class, 'title')]/text() | //div[contains(@class, 'zhicheng')]/text()
简介：//div[contains(@class, 'intro')]//text() | //div[contains(@class, 'desc')]//text()
擅长：//div[contains(@class, 'specialty')]//text() | //div[contains(@class, 'good-at')]//text()
```

### 2. 分页机制处理

**传统页码分页：**
```html
<div class="pagination">
    <a href="?page=1" class="prev">上一页</a>
    <a href="?page=1">1</a>
    <span class="current">2</span>
    <a href="?page=3">3</a>
    <a href="?page=3" class="next">下一页</a>
</div>
```

处理策略：
- 提取最大页码：`//div[@class='pagination']//a[last()-1]/text()`
- 构造分页URL：替换page参数
- 或点击分页按钮，等待新数据加载

**滚动加载：**
处理策略：
- 滚动到底部：`window.scrollTo(0, document.body.scrollHeight)`
- 等待新数据加载：监控元素数量变化
- 监控网络请求，找到数据API接口
- 循环直到没有新数据加载

**点击加载更多：**
```html
<button class="load-more-btn" onclick="loadMore()">加载更多</button>
```

处理策略：
- 点击按钮：`page.click('.load-more-btn')`
- 等待新元素：`page.wait_for_selector('.doctor-item:nth-child(20)')`
- 循环直到按钮消失或禁用

### 3. 动态内容处理

**识别动态加载：**
- 页面初始HTML内容很少
- 网络面板显示XHR/Fetch请求
- 元素在滚动或点击后出现

**Playwright处理策略：**
```python
# 等待元素加载
page.wait_for_selector('.doctor-item', timeout=10000)

# 滚动触发懒加载
for i in range(5):  # 滚动5次
    page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
    page.wait_for_timeout(1000)

# 点击展开按钮
expand_btn = page.query_selector('.expand-btn')
if expand_btn:
    expand_btn.click()
    page.wait_for_timeout(500)

# 网络拦截获取API数据
def handle_response(response):
    if '/api/doctor' in response.url:
        data = response.json()
        # 直接使用API返回的JSON数据

page.on('response', handle_response)
```

### 4. 反爬虫应对策略

**基础防护应对：**

| 防护手段 | 检测方式 | 应对策略 |
|---------|---------|---------|
| IP频率限制 | 请求返回429或验证码 | 控制QPS<3，随机延迟5-15秒 |
| User-Agent检测 | 默认UA被拒绝 | 模拟真实浏览器UA |
| Referer验证 | 直接访问被拒绝 | 添加来源页面Referer |
| Cookie验证 | 需要先访问首页 | 使用Session维护会话 |

**进阶防护应对：**

**代理IP池：**
- 轮换IP避免单IP封禁
- 每个IP维持低频请求
- 检测IP可用性，自动剔除失效IP

**浏览器指纹伪装：**
- 使用 playwright-stealth 插件
- 随机化浏览器特征
- 模拟真实用户行为

**行为模拟：**
- 随机滚动页面
- 鼠标随机移动
- 非规律性访问间隔

**验证码处理：**
- 记录验证码触发条件
- 暂停采集，后续人工处理
- 或接入第三方识别服务

---

## 三、队列配置

```
RABBITMQ_URL: amqp://guest:147258369aB@r.pisiewang.top:45672/
QUEUE_NAME: DoctorResult
```

---

## 四、推送数据格式

```json
{
  "name": "医生姓名",
  "hospital": "医院全称",
  "standard_department": "标准科室名",
  "display_department": "展示科室名",
  "title": "职称",
  "administrative_position": "行政职务",
  "academic_title": "学术头衔",
  "education": "学历",
  "degree": "学位",
  "alma_mater": "毕业院校",
  "years_of_experience": 28,
  "intro": "医生简介原文",
  "specialty": "擅长领域原文",
  "confidence_score": 100.0,
  "otherpropertys": "{\"confidence_name\": \"官网\", \"source_kind\": \"official\", \"seed_url\": \"官网首页URL\", \"root_domain\": \"域名\", \"url\": \"医生详情页URL\", \"match_score\": 1.0, \"page_record_time\": null, \"fetch_method\": \"playwright\"}",
  "source_model": "VW5W"
}
```

---

## 五、常见页面结构示例

### 示例1：表格布局医生列表页

**页面特征：**
- 医生信息以表格行列形式展示
- 表头包含：姓名、科室、职称、专业特长等
- 每行一个医生，信息规整

**提取步骤：**
1. 定位表格：`//table[contains(@class, 'doctor')]`
2. 遍历数据行：`//tbody/tr`
3. 提取每列数据：`./td[1]`, `./td[2]` 等
4. 收集详情链接：`./td[1]//a/@href`

### 示例2：卡片布局医生列表页

**页面特征：**
- 每个医生独立卡片
- 卡片包含头像、姓名、职称、科室
- 卡片底部有"查看详情"按钮

**提取步骤：**
1. 定位卡片容器：`//div[contains(@class, 'doctor-list')]`
2. 遍历卡片：`//div[contains(@class, 'doctor-card')]`
3. 从卡片各区域提取字段：
   - 头部：姓名、职称
   - 主体：科室、擅长
   - 底部：详情链接

### 示例3：医生详情页

**页面结构：**
```
顶部：医生照片 + 基本信息（姓名、职称、科室）
中部：专业特长区块
下部：个人简介区块（教育背景、工作经历）
底部：出诊信息表格
```

**提取策略：**
- 识别各信息区块的分隔（h2标题、section标签）
- 按区块提取对应字段
- 从简介文本中提取结构化字段

---

## 六、错误处理

1. **网络错误**：重试3次，间隔递增（1s, 2s, 4s）
2. **解析错误**：记录异常，跳过该医生，继续下一个
3. **推送失败**：本地备份，记录失败列表
4. **分页遍历**：设置最大页数限制（如50页），防止无限循环
5. **反爬虫拦截**：
   - 遇到验证码：暂停5分钟，记录URL
   - IP被封：切换代理IP，降低请求频率
   - 返回空页面：检查是否需要Cookie或特殊参数

---

## 七、注意事项

1. **只从官网获取数据**，不要查找第三方网站
2. **抓取所有可见医生**，不遗漏任何科室
3. **字段缺失时用 null 填充**，不要编造数据
4. **结构化字段提取要充分**，从简介中尽可能提取更多信息
5. **页面结构适应**：
   - 遇到未知布局时，先分析DOM结构
   - 记录有效的选择器策略
   - 建立常见模式知识库
6. **反爬虫合规**：
   - 遵守robots.txt协议
   - 控制采集频率，避免影响服务
   - 遇到验证码时暂停采集
7. **数据完整性**：
   - 优先保证核心字段（姓名、科室）
   - 次要字段缺失时填null
   - 记录字段覆盖率统计
8. **科室名称验证**：
   - 必须与医院官网的科室列表一致
   - 允许合理偏差（如"心血管内科"≈"心内科"）
   - 不匹配时记录原始值，standard_department填null

---

## 八、结构化字段提取详细示例

### 从业年限提取示例

```
原文：从事眼科临床工作28年
提取：years_of_experience = 28
正则：从事.*?(\d+).*?年

原文：从业临床工作30余年
提取：years_of_experience = 30
规则：忽略"余"、"多"等修饰词

原文：从医20余年
提取：years_of_experience = 20
规则：取第一个匹配的数字

原文：2009年获得中药学本科学历，从事医学影像工作30年
提取：years_of_experience = 30
规则：匹配"从事XX工作N年"模式
```

### 学历学位提取示例

```
原文：医学博士、博士后
提取：education = "博士研究生", degree = "博士"

原文：研究生学历，博士学位
提取：education = "博士研究生", degree = "博士"

原文：硕士研究生
提取：education = "硕士研究生", degree = "硕士"

原文：大学专科，毕业于菏泽医专医疗专业
提取：education = "大专", degree = null

优先级：博士研究生 > 硕士研究生 > 本科 > 大专
```

### 毕业院校提取示例

```
原文：1992年毕业于泰山医学院、医学影像系
提取：alma_mater = "泰山医学院"
正则：毕业于(.{2,10}大学|.{2,10}医学院)

原文：北京医科大学眼科学博士
提取：alma_mater = "北京医科大学"
规则：识别大学名称

原文：山东大学
提取：alma_mater = "山东大学"
规则：直接匹配大学名称
```

### 行政职务提取示例

```
原文：现任临沂市中心医院影像科副主任，副主任技师
提取：administrative_position = "副主任"
规则：匹配"XX科主任"、"XX科副主任"

原文：骨科医院院长，骨关节科主任
提取：administrative_position = "院长"
优先级：院长 > 副院长 > 主任 > 副主任

原文：眼底病科主任
提取：administrative_position = "主任"
```

### 学术头衔提取示例

```
原文：主任医师、教授、博士生导师
提取：academic_title = "教授、博士生导师"
规则：提取除职称外的学术头衔，用顿号连接

原文：博士研究生导师，齐鲁卫生与健康领军人才
提取：academic_title = "博士研究生导师、齐鲁卫生与健康领军人才"

常见学术头衔：
- 教授、副教授
- 博士生导师、硕士生导师
- 享受国务院津贴
- 齐鲁卫生与健康领军人才
- 省级学科带头人
```

---

## 九、字段映射参考表

| 目标字段 | 提取位置 | 常见模式 | 必需 | 示例 |
|---------|---------|---------|------|------|
| name | 标题/首行 | 张三、李医生 | ✅ | //h1/text() |
| hospital | 消息参数 | 医院全称 | ✅ | 从参数获取 |
| standard_department | 科室列表 | 标准科室名 | ⚠️ | 匹配科室列表 |
| display_department | 详情页 | 展示科室名 | ✅ | //div[@class='dept']/text() |
| title | 标签/简介 | 主任医师 | ⚠️ | 正则匹配 |
| administrative_position | 简介文本 | 科室主任 | ❌ | 正则提取 |
| academic_title | 简介文本 | 教授、博导 | ❌ | 正则提取 |
| education | 教育背景 | 博士研究生 | ❌ | 正则匹配 |
| degree | 教育背景 | 医学博士 | ❌ | 正则匹配 |
| alma_mater | 教育背景 | 北京医科大学 | ❌ | 正则提取 |
| years_of_experience | 简介文本 | 从事XX工作28年 | ❌ | 正则提取数字 |
| intro | 正文区域 | 个人简介 | ✅ | //div[@class='intro']/text() |
| specialty | 专业领域 | 擅长疾病列表 | ⚠️ | //div[@class='specialty']/text() |
| confidence_score | - | 置信度评分 | ✅ | 默认100.0 |
| otherpropertys | - | 元数据JSON | ✅ | 构造JSON字符串 |
| source_model | - | 数据来源模型 | ✅ | 固定"VW5W" |

**字段必需级别：**
- ✅ 必需：必须提取，缺失则不推送
- ⚠️ 重要：尽量提取，缺失时填null但仍推送
- ❌ 可选：尽力提取，缺失时填null

---