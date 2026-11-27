@echo off
title Giam Sat He Thong Phan Tan Apache Spark - Web UI 4040
echo ==============================================================
echo   CHUONG TRINH GIAM SAT HE THONG PHAN TAN SPARK (WEB UI)
echo ==============================================================
echo.
echo He thong se nap gio hang hop le (orders_prior) vao Apache Spark
echo va giu phien lam viec de ban mo trinh duyet xem truc quan tai:
echo.
echo           http://localhost:4040
echo.
echo ==============================================================
echo.
.\.venv\Scripts\python.exe scripts/monitor_spark_ui.py
pause
