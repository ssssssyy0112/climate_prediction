# 数据集
## ECA&D 57 G
- ECA 数据集包含整个欧洲和地中海气象站的一系列日常观测数据。
- 时间：1882年-2023年9月
- **日**为单位的数据
- 官网：[Home European Climate Assessment & Dataset](https://www.ecad.eu/)
### 数据组成

1. Daily maximum temperature TX（每日最高气温）：每天的最高气温，通常以摄氏度（°C）或华氏度（°F）表示。它表示一天中气温的最高点。
2. Daily minimum temperature TN（每日最低气温）：每天的最低气温，通常以摄氏度（°C）或华氏度（°F）表示。它表示一天中气温的最低点。
3. Daily mean temperature TG（每日平均气温）：每天的平均气温，通常以摄氏度（°C）或华氏度（°F）表示。它表示一天中整个时间段内的气温平均值。
4. Daily precipitation amount RR（每日降水量）：每天的降水量，通常以毫米（mm）或英寸（in）表示。它表示一天内降水的总量，包括雨水、雪、雨夹雪等。
5. Daily mean sea level pressure PP（每日平均海平面气压）：每天的平均海平面气压，通常以希帕斯卡尔（hPa）或百帕斯卡（mb）表示。它表示一天中的大气气压平均值，通常用于天气预报。
6. Daily cloud cover CC（每日云量）：每天的云量，通常以百分比表示。它表示一天中天空被云层覆盖的程度。
7. Daily humidity HU（每日湿度）：每天的湿度，通常以百分比表示。它表示空气中水蒸气的含量，用于描述空气的湿润程度。
8. Daily snow depth SD（每日雪深）：每天的雪深，通常以厘米（cm）或英寸（in）表示。它表示积雪覆盖地面的深度。
9. Daily sunshine duration SS（每日日照时数）：每天的日照时数，通常以小时表示。它表示一天中太阳照射地表的时间长度。
10. Global radiation QQ（全球辐射）：全球辐射是指地球表面接收到的来自太阳的辐射总量，通常以焦耳每平方米（J/m²）或千卡每平方厘米（kcal/cm²）表示。
11. Daily mean wind speed FG（每日平均风速）：每天的平均风速，通常以米每秒（m/s）或英里每小时（mph）表示。它表示一天中风的平均速度。
12. Daily maximum wind gust FX（每日最大瞬时风速）：每天的最大瞬时风速，通常以米每秒（m/s）或英里每小时（mph）表示。它表示一天中风的最大瞬时速度。
13. Daily wind direction DD（每日风向）：每天的风向，通常以度数表示。它表示一天中风吹的方向，通常是相对于正北方向的度数。
### 数据格式
- **城市**，数据来源，**日期**，**测量值**，置信值
![image-20231117211728472](./%E6%B0%94%E6%B8%A9-%E5%A4%A7%E4%BD%9C%E4%B8%9A.assets/image-20231117211728472.png)
- 在ECA&D（European Climate Assessment & Dataset）数据集中，温度的单位通常是以0.1摄氏度（0.1°C）为基准的。这意味着数据集中的温度值是实际温度的十倍。例如，一个数据集中的温度值为290，实际上代表的是29.0°C。这种表示方式使得数据集可以更精确地表示温度，而不需要使用小数点。