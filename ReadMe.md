# Реализация Kafka на Go

A Kafka implementation using , the  package, and the example provided in the kafka-go package.

Простая реализация _Kafka_ на _Golang_ с использованием пакета [kafka-go](https://github.com/segmentio/kafka-go) и [UI for Apache Kafka](https://github.com/provectus/kafka-ui)

> [!NOTE]
>Apache Kafka — это Open Source-платформа для распределенной потоковой передачи событий. Ее задача — организация  высокопроизводительных пайплайнов данных, потоковая аналитика, интеграция данных.

> [!NOTE]
>UI for Apache Kafka — это веб-сервис с открытым кодом и понятным пользовательским интерфейсом для работы с кластерами Apache Kafka. Он позволяет разработчикам отслеживать потоки, находить и устранять проблемы с данными.

## Запуск программы и контейнера

Для сборки docker-compose.yml перейдите в директорию проекта и в терминале запустите:

    % docker-compose up

Затем запустите программу командой

    % go run main.go

Параллельно в браузере перейдите на ```http://localhost:8080/```, откроется интерфейс UI for Apache Kafka, в котором будет видно, что три раза в секунду происходит событие.
