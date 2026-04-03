# 餐饮点评趋势预测与知识图谱可视化系统

## 快速启动

1. 复制配置文件：
```bash
copy .env.example .env
```

可选（国内网络建议）：启用 Hugging Face 镜像
```powershell
$env:HF_ENDPOINT="https://hf-mirror.com"
```
也可以写入 `.env`：
```env
HF_ENDPOINT=https://hf-mirror.com
```

2. 安装依赖：
```bash
pip install -r requirements.txt
```

3. 初始化数据库与图数据库约束：
```bash
python scripts/init_db.py
python scripts/init_neo4j.py
```

4. 生成并导入 mock 数据（推荐：2家店、每家1000条）：
```bash
python scripts/reset_data.py
python scripts/generate_mock_data.py --shop-count 2 --per-shop 1000 --output data/mock_reviews.json
python scripts/import_reviews.py --file data/mock_reviews.json
```

可选：导入真实 CSV 数据集（会自动过滤 `restaurants.csv` 里 `name` 为空的 `restId`）：
```bash
python scripts/reset_data.py
python scripts/import_reviews.py ^
  --ratings-csv ratings/ratings/ratings.csv ^
  --restaurants-csv ratings/ratings/restaurants.csv ^
  --chunk-size 20000 ^
  --max-rows 5000
```

说明：
- `--max-rows 5000` 是默认值，先小批量验证最稳；
- 若要全量导入可用 `--max-rows -1`（耗时会很长）。

5. 启动后端：
```bash
python backend/run.py
```

6. 打开前端（推荐同源访问，避免 file:// 跨域）：
```bash
python backend/run.py
```
然后浏览器访问：
`http://127.0.0.1:5000/`

## API

- `GET /api/reviews`
- `POST /api/import`
- `GET /api/graph`
- `POST /api/predict`
- `GET /api/workflow`

## 学术版分析链路（当前实现）

1. 数据接入：`ratings.csv + restaurants.csv` 入库，自动过滤 `name` 为空的 `restId`。  
2. NLP 因子化：评论文本抽取属性标签（口味/服务/环境/价格等）并计算情感分值。  
3. 因子知识图谱：`Shop-Review-Factor` 三层结构，边权记录 `effect/polarity/mention_count`。  
4. 趋势预测：LSTM 使用 `rating + sentiment + review_count + (env/flavor/service)` 多变量序列预测。  
5. 解释对齐：前端同时展示预测曲线、属性影响条形图、因子图谱，形成“预测结果 -> 影响因子”的闭环解释。

说明：真实数据集没有菜品字段，系统会将 `dish` 统一写为 `N/A`，前端解释以“店铺 + 属性因子”为主。
