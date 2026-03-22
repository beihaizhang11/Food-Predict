# 餐饮点评趋势预测与知识图谱可视化系统

## 快速启动

1. 复制配置文件：
```bash
copy .env.example .env
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
