# RabbitMQ WebShovel Plugin

Плагин RabbitMQ осуществляющий публикацию сообщений посредством WebAPI или SOAP

## Build

    $ make app

## Configuration
### Static configuration
Общее определение:
```erlang
{rabbitmq_webshovel, [{webshovels, [{webshovel_name, [ ... ]}, ... ]}]}
```

Спискок "webshovels" содержит именованные статические конфигурации. Кажддое наименование *webshovel_name* должено быть уникальным.
Каждое определение webshovel выглядит следующим образом
```erlang
{*webshovel_name,
    [{source, [ ... protocol specific config ... ]},
     {destinations, [ ... destinations specific config ...]},
     {reconnect_delay, *reconn_delay}
    ]}
```
где *webshovel_name* имя конфигурации (атом Erlang)

*source* и *destinations* являются обязательными элементами конфигурации
*webshovel_name* - Название webshovel (атом Erlang).

#### reconect_delay - этот раздел является не обязательным
    
В определении `{reconnect_delay, reconn_delay}` *reconn_delay* - это количество секунд ожидания перед повторным подключением в случае сбоя соединения
```erlang
{reconnect_delay, 1.5}
```
будет осуществлена задержка на 1.5 секунд до повторного подключения после обрыва соединения.
    Если *reconn_delay* равно 0, то повторное подключение иницировано не будет после обрыва связи.
    Значение *reconn_delay* по умолчанию 5 секунд
#### source - этот раздел является обязательным
Раздел имеет разные свойства для разных протоколов. Общими для всех протоколов свойствами являются:
```erlang
{source,
    [
        {protocol, amqp091/amqp10},
        {uris, *uri_list}
    ]}
```
##### protocol
Этот раздел является обязательным и может принимать одно из двух значений: *amqp091* или *amqp10*
##### URIs
Этот раздел является обязательным. В
```erlang
{uris, *uri_list}
```
*uri_list* - список URI брокера для подключения. Пример:
```erlang
[ "amqp://fred:secret@host1.domain/my_vhost", 
  "amqp://john:secret@host2.domain/my_vhost"
]
``` 
Если *host* опущен, используется прямое соединение с брокером.
