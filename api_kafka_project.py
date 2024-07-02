import pandas as pd  # Импорт библиотеки pandas для работы с данными в виде таблиц
import json  # Импорт модуля json для работы с JSON данными
import numpy as np  # Импорт библиотеки numpy для работы с массивами и матрицами
from kafka.admin import KafkaAdminClient, NewTopic  # Импорт необходимых классов для управления Kafka топиками
from kafka import KafkaProducer, KafkaConsumer  # Импорт классов KafkaProducer и KafkaConsumer для работы с Kafka


class KafkaTopicAPI:  # Объявление класса KafkaTopicAPI
    def __init__(self, bootstrap_servers='localhost:9092'):  # Метод инициализации класса с параметром bootstrap_servers
        self.bootstrap_servers = bootstrap_servers  # Инициализация переменной bootstrap_servers
        self.admin_client = KafkaAdminClient(bootstrap_servers=self.bootstrap_servers, client_id='topic-api')  # Создание объекта KafkaAdminClient

    def create_topic(self, topic_name):  # Метод для создания Kafka топика
        topic_list = [NewTopic(name=topic_name, num_partitions=1, replication_factor=1)]  # Создание списка NewTopic объектов
        self.admin_client.create_topics(new_topics=topic_list, validate_only=False)  # Создание Kafka топика
        print(f"Topic '{topic_name}' created.")  # Вывод сообщения о создании топика

    def write_data_to_topic(self, topic_name, data):  # Метод для записи данных в Kafka топик
        producer = KafkaProducer(bootstrap_servers=[self.bootstrap_servers])  # Создание объекта KafkaProducer
        for chunk in np.array_split(data, len(data)):  # Цикл по частям данных
            dict_to_kafka = chunk.to_dict(orient='records')  # Преобразование данных в словарь
            for entry in dict_to_kafka:  # Цикл по записям в словаре
                data = json.dumps(entry, default=str).encode('utf-8')  # Преобразование данных в JSON формат и кодирование
                producer.send(topic=topic_name, value=data)  # Отправка данных в Kafka топик
        producer.flush()  # Очистка буфера KafkaProducer
        print(f"Data written to topic '{topic_name}'.")  # Вывод сообщения

    def read_data_from_topic(self, topic_name):  # Метод для чтения данных из Kafka топика
        consumer = KafkaConsumer(  # Создание объекта KafkaConsumer для чтения сообщений из Kafka топика
            topic_name,  # Указание имени Kafka топика, из которого будут читаться сообщения
            group_id='topic-api-consumer-group',  # Уникальный идентификатор группы потребителей для управления читателями
            bootstrap_servers=[self.bootstrap_servers],  # Указание серверов Kafka, к которым будет устанавливаться соединение
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),  # Функция для десериализации значения сообщения из JSON формата
            auto_offset_reset='earliest',  # Установка начальной позиции чтения на начало топика (earliest) при первом запуске
            enable_auto_commit=False  # Отключение автоматического подтверждения (commit) сообщений
        )  # Завершение параметров и создание объекта KafkaConsumer# Создание объекта KafkaConsumer
        
        messages = []  # Инициализация списка сообщений
        for message in consumer:  # Цикл по сообщениям
            messages.append(message.value)  # Добавление значения сообщения в список
        
        df = pd.DataFrame(messages)  
        print(f"Data read from topic '{topic_name}'.")  # Вывод сообщения

        return df  

    def delete_topic(self, topic_name):  # Метод для удаления Kafka топика
        self.admin_client.delete_topics(topics=[topic_name])  # Удаление Kafka топика
        print(f"Topic '{topic_name}' deleted.")  # Вывод сообщения


# Пример использования:
api = KafkaTopicAPI()

# Создание топика
api.create_topic("Pokemon")

# # Запись данных в топик
# data = pd.read_csv('Pokemon.csv', delimiter=';')
# api.write_data_to_topic("Pokemon", data)

# # Чтение данных из топика
# read_data = api.read_data_from_topic("Pokemon")
# print(read_data)

# # Удаление топика
# api.delete_topic("Pokemon")