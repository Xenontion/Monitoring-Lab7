import time
import random
import pika
import redis
from pymongo import MongoClient

# ==========================================
# 1. НАВАНТАЖЕННЯ ТА МОНІТОРИНГ RABBITMQ
# ==========================================
def test_rabbitmq():
    print("\n" + "=" * 50)
    print("[1/3] Запуск навантаження RabbitMQ...")
    print("=" * 50)
    
    connection_params = pika.ConnectionParameters(
        host='localhost', 
        port=5672, 
        credentials=pika.PlainCredentials('guest', 'guest')
    )
    
    conn = pika.BlockingConnection(connection_params)
    channel = conn.channel()
    channel.queue_declare(queue='orders', durable=True)
    
    # Продюсер: надсилання 200 повідомлень
    for i in range(1, 201):
        body = f'{{"order_id": {i}, "amount": {random.randint(10, 500)}}}'
        channel.basic_publish(
            exchange='',
            routing_key='orders',
            body=body,
            properties=pika.BasicProperties(delivery_mode=2)
        )
    print("  -> RabbitMQ: Відправлено 200 повідомлень у чергу 'orders'.")

    # Консьюмер через неблокуючий basic_get:
    # 50 повідомлень підтверджуємо (ACK)
    for _ in range(50):
        method_frame, _, _ = channel.basic_get(queue='orders', auto_ack=False)
        if method_frame:
            channel.basic_ack(delivery_tag=method_frame.delivery_tag)

    # 10 повідомлень отримуємо та навмисно НЕ підтверджуємо (формуємо unacked)
    for _ in range(10):
        channel.basic_get(queue='orders', auto_ack=False)

    print("  -> RabbitMQ: Опрацьовано 50 повідомлень (ACK), 10 залишено в статусі unacked.")
    
    # Невелика пауза для фіксації стану в брокері
    time.sleep(2)
    conn.close()

# ==========================================
# 2. НАВАНТАЖЕННЯ ТА МОНІТОРИНГ MONGODB
# ==========================================
def test_mongodb():
    print("\n" + "=" * 50)
    print("[2/3] Запуск навантаження та профілювання MongoDB...")
    print("=" * 50)
    
    client = MongoClient("mongodb://localhost:27017/")
    db = client["monitoring_db"]
    collection = db["users"]
    
    # 1. Увімкнення профілювання через команду рівня бази даних (рівень 1, slowms=10)
    db.command("profile", 1, slowms=10)
    print("  -> MongoDB: Профілювання активовано (рівень 1, slowms=10).")
    
    # 2. Очищення та вставка 15 000 документів
    collection.drop()
    docs = [
        {"user_id": i, "name": f"User_{i}", "category": f"cat_{i % 10}", "payload": "x" * 200}
        for i in range(1, 15001)
    ]
    collection.insert_many(docs)
    print(f"  -> MongoDB: Додано {len(docs)} документів у колекцію 'users'.")
    
    # 3. Виконання неіндексованих запитів для гарантованого COLLSCAN
    time.sleep(0.5)
    for _ in range(5):
        _ = list(collection.find({"name": {"$regex": ".*User_149.*"}}))
        
    # 4. Отримання повільних запитів із system.profile
    print("\n--- ДІАГНОСТИКА MONGODB: Повільні запити (system.profile) ---")
    slow_queries = list(db.system.profile.find({"millis": {"$gt": 0}}).sort("millis", -1).limit(3))
    
    if slow_queries:
        for q in slow_queries:
            plan = q.get("planSummary", "N/A")
            millis = q.get("millis", 0)
            ns = q.get("ns", "N/A")
            print(f"  [Колекція: {ns}] Час виконання: {millis}ms | План: {plan}")
    else:
        print("  Повільних запитів тривалістю понад 10 мс не зафіксовано.")

    # 5. Статистика колекції через команду collStats
    stats = db.command("collStats", "users")
    print(f"  -> MongoDB Stats: Документів: {stats.get('count')}, Розмір даних: {stats.get('size')} байт, Індексів: {stats.get('nindexes')}")

# ==========================================
# 3. НАВАНТАЖЕННЯ ТА МОНІТОРИНГ REDIS
# ==========================================
def test_redis():
    print("\n" + "=" * 50)
    print("[3/3] Запуск навантаження та аналізу Redis...")
    print("=" * 50)
    
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    
    # 1. Запис 10 000 ключів із встановленим TTL через pipeline
    pipe = r.pipeline()
    for i in range(1, 10001):
        pipe.set(f"cache:key:{i}", f"value_{i}_payload_data", ex=3600)
    pipe.execute()
    print("  -> Redis: Записано 10,000 ключів із TTL.")
    
    # 2. Виконання блокуючої команди KEYS * (потрапляє в SLOWLOG)
    print("  -> Redis: Виконання важкої операції 'KEYS *'...")
    _ = r.keys("cache:key:*")
    
    # 3. Збір метрик INFO
    info_mem = r.info("memory")
    info_stats = r.info("stats")
    info_clients = r.info("clients")
    
    print("\n--- ДІАГНОСТИКА REDIS: Метрики стану (INFO) ---")
    print(f"  Використано пам'яті:      {info_mem.get('used_memory_human')}")
    print(f"  Пікове споживання пам'яті: {info_mem.get('used_memory_peak_human')}")
    print(f"  Фрагментація пам'яті:      {info_mem.get('mem_fragmentation_ratio')}")
    print(f"  Активних клієнтів:        {info_clients.get('connected_clients')}")
    print(f"  Всього команд виконано:   {info_stats.get('total_commands_processed')}")
    
    # 4. Читання записів повільних команд із SLOWLOG
    print("\n--- ДІАГНОСТИКА REDIS: Slowlog (Останні повільні команди) ---")
    slow_logs = r.slowlog_get(5)
    for log in slow_logs:
        raw_cmd = log.get('command', [])
        # Декодування аргументів команди в читабельний текст
        decoded_cmd = []
        for arg in raw_cmd[:4]:
            if isinstance(arg, bytes):
                decoded_cmd.append(arg.decode('utf-8', errors='ignore'))
            else:
                decoded_cmd.append(str(arg))
        cmd_str = " ".join(decoded_cmd)
        print(f"  ID: {log.get('id')} | Тривалість: {log.get('duration')} мкс | Команда: {cmd_str}...")

# ==========================================
# ТОЧКА ВХОДУ
# ==========================================
if __name__ == "__main__":
    test_rabbitmq()
    test_mongodb()
    test_redis()
    print("\n" + "=" * 50)
    print("Усі тести завершено успішно. Дані готові для звіту.")
    print("=" * 50)