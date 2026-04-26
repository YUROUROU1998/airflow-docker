# Airflow DAG - 接受檔案排程dag1

## Infor
某來源系統會在每天在 00:00 UTC+8 時,拋送 record_yyyymmdd.txt 的檔案, 其中
yyyymmdd 為檔案拋送日(e.g. 今天是 2025/03/24, 檔案名稱則為
record_20250324.txt)至路徑 /tmp/test/ 底下,請設計一個排程每天接收
record_yyyymmdd.txt
該檔正常最晚會在 00:30 UTC+8 前送至路徑,若晚於 00:30 到檔,則接收排程失敗
接檔時,須檢查檔案符合以下三個條件,確保資料內容正常。
1. size > 50 bytes
2. header 以外的資料筆數 >0
3. id 欄位值不重複

若任一條件不符合,則排程失敗並需寄發告警信,信件內容需包含失敗 task 的
task_id、排程執行日、exception 內容
若排程執行成功,則將該檔案移至路徑 /tmp/success/底下
DAG 需回朔兩天前的資料,僅需參考 airflow 所提供的 operator,不需使用客製化的
operator

## DAG flow
wait_file >> check_file_task >> [move_file, send_email]
FileSensor/PythonOperator/BashOperator/EmailOperator


## Task definition

| Task_id | Operator | Description |
|---|---|---|
| `wait_file` | FileSensor | 等待 record_yyyymmdd.txt 出現，最多等 30 分鐘 |
| `check_file_task` | PythonOperator | 檢查檔案：大小 > 50 bytes、資料筆數 > 0、id 不重複 |
| `send_email` | EmailOperator | 驗證失敗時寄送告警信，包含 task_id、執行日、錯誤內容 |
| `move_file` | BashOperator | 驗證成功時將檔案移至 /tmp/success/ |

## Setting

| Setting | value | Description |
|---|---|---|
| `schedule_interval` | `0 16 * * *` | 每天 00:00 UTC+8 執行 |
| `start_date` | `2026-04-22` | 回朔兩天 |
| `catchup` | `True` | 補跑 |

## Condition

1. size > 50 bytes
2. header 以外的資料筆數 > 0
3. id 欄位值不重複

## fail email content
失敗時，`send_email` 會寄發告警信：
- Task ID
- 排程執行日
- Exception


## 執行方式

Refer to [執行說明]


## Tree

```
airflow-docker/
├── dags/
│   └── dag1.py               # airflow DAG 原始碼
├── logs/                     
├── plugins/                  
├── config/                   
├── sample_data/
│   └── record_20260422.txt
│   └── record_20260423.txt   # 假資料
├── docker-compose.yaml       # docker-compose.yml
└── README.md                 # README
```
