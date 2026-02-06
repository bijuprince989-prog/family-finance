import os
import random
import string
from datetime import datetime
from typing import List, Optional

import pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

app = FastAPI()

# --- 1. 数据库配置 (自动切换) ---
# 如果是 Render 部署，它会提供 DATABASE_URL 环境变量
DATABASE_URL = os.getenv("DATABASE_URL")

if DATABASE_URL:
    # 兼容 Render 的 postgres:// 前缀 (SQLAlchemy 要求 postgresql://)
    if DATABASE_URL.startswith("postgres://"):
        DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
    # 连接云端 PostgreSQL
    engine = create_engine(DATABASE_URL)
else:
    # 连接本地 SQLite
    engine = create_engine("sqlite:///finance.db", connect_args={"check_same_thread": False})

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def init_db():
    with engine.connect() as conn:
        # 用户表
        conn.execute(
            text('CREATE TABLE IF NOT EXISTS users (id SERIAL PRIMARY KEY, username TEXT UNIQUE, password TEXT)'))
        # 账本表
        conn.execute(text('CREATE TABLE IF NOT EXISTS groups (group_id TEXT PRIMARY KEY, creator_id INTEGER)'))
        # 权限表
        conn.execute(
            text('CREATE TABLE IF NOT EXISTS memberships (user_id INTEGER, group_id TEXT, UNIQUE(user_id, group_id))'))
        # 分类表
        conn.execute(text(
            'CREATE TABLE IF NOT EXISTS categories (id SERIAL PRIMARY KEY, group_id TEXT, type TEXT, name TEXT, UNIQUE(group_id, type, name))'))
        # 记录表
        conn.execute(text('''CREATE TABLE IF NOT EXISTS records (
            id SERIAL PRIMARY KEY, user_id INTEGER, amount REAL, 
            type TEXT, category TEXT, note TEXT, time TEXT, group_id TEXT)'''))
        conn.commit()


init_db()


# --- 2. 权限校验 ---
def has_access(username: str, group_id: str):
    with engine.connect() as conn:
        res = conn.execute(text('''SELECT 1 FROM memberships m JOIN users u ON m.user_id = u.id 
                                 WHERE u.username = :u AND m.group_id = :g'''),
                           {"u": username, "g": group_id}).fetchone()
        return res is not None


# --- 3. 数据模型 ---
class AddRecord(BaseModel):
    username: str
    amount: float
    type: str
    category: str
    note: str = ""
    time: str
    group_id: str


# --- 4. 接口实现 ---

@app.post("/register")
def register(user: dict):
    try:
        with engine.begin() as conn:
            conn.execute(text("INSERT INTO users (username, password) VALUES (:u, :p)"),
                         {"u": user['username'], "p": user['password']})
        return {"message": "成功"}
    except:
        raise HTTPException(status_code=400)


@app.post("/login")
def login(user: dict):
    with engine.connect() as conn:
        res = conn.execute(text("SELECT username FROM users WHERE username = :u AND password = :p"),
                           {"u": user['username'], "p": user['password']}).fetchone()
        if res: return {"status": "success", "username": res[0]}
    raise HTTPException(status_code=401)


@app.post("/create_group")
def create_group(username: str):
    code = ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))
    with engine.begin() as conn:
        uid = conn.execute(text("SELECT id FROM users WHERE username = :u"), {"u": username}).fetchone()[0]
        conn.execute(text("INSERT INTO groups (group_id, creator_id) VALUES (:g, :c)"), {"g": code, "c": uid})
        conn.execute(text("INSERT INTO memberships (user_id, group_id) VALUES (:u, :g)"), {"u": uid, "g": code})
    return {"invite_code": code}


@app.post("/join_group")
def join_group(username: str, invite_code: str):
    with engine.begin() as conn:
        uid = conn.execute(text("SELECT id FROM users WHERE username = :u"), {"u": username}).fetchone()[0]
        exists = conn.execute(text("SELECT 1 FROM groups WHERE group_id = :g"), {"g": invite_code}).fetchone()
        if not exists: raise HTTPException(status_code=404)
        try:
            conn.execute(text("INSERT INTO memberships (user_id, group_id) VALUES (:u, :g)"),
                         {"u": uid, "g": invite_code})
        except:
            pass
    return {"message": "成功"}


@app.get("/get_my_groups")
def get_my_groups(username: str):
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT group_id FROM memberships m JOIN users u ON m.user_id = u.id WHERE u.username = :u"),
            {"u": username}).fetchall()
        return {"groups": [r[0] for r in rows]}


@app.get("/search_records")
def search_records(username: str, group_id: Optional[str] = None, year: Optional[int] = None,
                   month: Optional[int] = None, day: Optional[int] = None, filter_type: str = "全部"):
    if not group_id or not has_access(username, group_id):
        return {"data": [], "summary": {"income": 0, "expense": 0, "balance": 0}}

    pattern = f"{year if year else ''}-{f'{month:02d}' if month else ''}-{f'{day:02d}' if day else ''}%".strip(
        "-%") + "%"

    with engine.connect() as conn:
        sql = "SELECT r.*, u.username FROM records r JOIN users u ON r.user_id = u.id WHERE r.group_id = :g AND r.time LIKE :p"
        params = {"g": group_id, "p": pattern}
        if filter_type != "全部":
            sql += " AND r.type = :t"
            params["t"] = filter_type
        sql += " ORDER BY r.time DESC, r.id DESC"

        df = pd.read_sql_query(text(sql), conn, params=params)

    if df.empty: return {"data": [], "summary": {"income": 0, "expense": 0, "balance": 0}}
    inc = df[df['type'] == '收入']['amount'].sum()
    exp = df[df['type'] == '支出']['amount'].sum()
    return {"data": df.to_dict(orient="records"),
            "summary": {"income": float(inc), "expense": float(exp), "balance": float(inc - exp)}}


@app.get("/get_analytics")
def get_analytics(username: str, group_id: Optional[str] = None, year: int = 2026, month: int = 2):
    if not group_id or not has_access(username, group_id): return {"income": [], "expense": []}
    pattern = f"{year}-{month:02d}%"
    with engine.connect() as conn:
        sql = 'SELECT type, category, SUM(amount) as total FROM records WHERE group_id = :g AND time LIKE :p GROUP BY type, category'
        df = pd.read_sql_query(text(sql), conn, params={"g": group_id, "p": pattern})

    if df.empty: return {"income": [], "expense": []}
    return {"income": df[df['type'] == '收入'].to_dict(orient="records"),
            "expense": df[df['type'] == '支出'].to_dict(orient="records")}


@app.post("/add_record")
def add_record(data: AddRecord):
    if not has_access(data.username, data.group_id): raise HTTPException(status_code=403)
    with engine.begin() as conn:
        uid = conn.execute(text("SELECT id FROM users WHERE username = :u"), {"u": data.username}).fetchone()[0]
        conn.execute(text(
            'INSERT INTO records (user_id, amount, type, category, note, time, group_id) VALUES (:u, :a, :t, :c, :n, :tm, :g)'),
                     {"u": uid, "a": data.amount, "t": data.type, "c": data.category, "n": data.note, "tm": data.time,
                      "g": data.group_id})
    return {"message": "成功"}


@app.get("/get_categories")
def get_categories(group_id: Optional[str] = None, type: str = "支出"):
    if not group_id: return []
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT name FROM categories WHERE group_id = :g AND type = :t"),
                            {"g": group_id, "t": type}).fetchall()
        return [r[0] for r in rows]


@app.post("/add_category")
def add_category(group_id: str, type: str, name: str):
    with engine.begin() as conn:
        try:
            conn.execute(text("INSERT INTO categories (group_id, type, name) VALUES (:g, :t, :n)"),
                         {"g": group_id, "t": type, "n": name})
        except:
            pass
    return {"message": "成功"}


@app.delete("/delete_record")
def delete_record(record_id: int):
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM records WHERE id = :id"), {"id": record_id})
    return {"message": "成功"}


@app.get("/get_summary")
def get_summary(username: str, group_id: str, year: int, month: int):
    return search_records(username, group_id, year, month)["summary"]


@app.get("/get_records")
def get_records(username: str, group_id: str):
    res = search_records(username, group_id)
    return {"data": res["data"][:10]}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)