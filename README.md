# go-idempotency

Идемпотентность для `user`, `bank-statement-data` и `assistant` ([FIN-78](https://finassist.youtrack.cloud/issue/FIN-78)). Библиотека извлекает ключ из metadata, хранит protobuf-ответ или контрактную ошибку вместе с эффектом в транзакции сервиса, воспроизводит ответ и очищает просроченные записи. Для `AskQuestion` есть отдельный короткий резерв с fencing. Источник правил — [grpc-api.md, разделы 6 и 7.4](https://github.com/fin-assist/contracts/blob/main/docs/grpc-api.md) и [архитектура](https://finassist.atlassian.net/wiki/spaces/PF/pages/5210132).

## Внедрение

### 1. Зависимость и миграции

После выпуска владельцем закрепите опубликованную версию: `go get github.com/fin-assist/go-idempotency@vX.Y.Z`. До выпуска для локальной проверки допустим `replace` на checkout библиотеки; в PR сервиса он не попадает.

В `MigrationSources()` сервиса перед собственным источником:

```go
{Name: idempotency.MigrationsName, Table: idempotency.MigrationsTable, FS: idempotency.Migrations},
{Name: "service", Table: "schema_migrations", FS: migrations.FS},
```

Примените `migrate up`. Источник `idempotency`, таблица версий `idempotency_schema_migrations`; `001_keys` создаёт `idempotency_keys` с PK `(scope, endpoint, key)` и индексом срока. Down удаляет таблицу и сохранённые результаты: это операция отката схемы, не способ очистки.

Все операции — **на primary (`-rw`), READ COMMITTED**. Передавайте `postgres.DB` через интерфейс `Tx(context.Context) (pgx.Tx, bool)`. Отсутствующая транзакция — `ErrNoTransaction`; библиотека не начинает, не коммитит и не откатывает её. На `-ro` библиотеку вызывать нельзя. Не делайте внешних вызовов внутри транзакции эффекта.

### 2. Сборка и интерцептор

```go
cipher, err := idempotency.NewCipher(refreshKey) // AES key from the service secret: 16/24/32 bytes
if err != nil { return err }
idem, err := idempotency.New(db, idempotency.Options{
    Methods: map[string]idempotency.Policy{
        "/finassist.user.v1.AuthService/StartPhoneVerification": {Encrypt: true},
        "/finassist.user.v1.AuthService/Register": {Encrypt: true},
        "/finassist.user.v1.AuthService/RefreshTokens": {TTL: 60*time.Second, Encrypt: true},
    },
    Clock: time.Now,
    Cipher: cipher,
    Registerer: registry,
    LockTimeout: time.Second,
    EffectTimeout: 30*time.Second,
})
if err != nil { return err }
```

Таблица содержит только точные полные имена методов этого сервиса. Другие методы интерцептор пропускает. Политики копируются; после `New` их не меняют. Один экземпляр метрик на реестр; повторная регистрация — ошибка.

Порядок unary chain: сервисная/пользовательская идентичность → tombstone → `idem.UnaryInterceptor(scope)` → обработчик. `scope(ctx)` возвращает `user_id` **из проверенного токена**, для методов без пользовательского токена — пустую строку. При недостающей обязательной идентичности он возвращает ошибку, а не пустой scope. Не читайте `user_id` из тела или произвольной metadata. В БД пустой scope представлен nil-UUID. Ключ — ровно одно значение `idempotency-key`, UUID; отсутствующий, повторный или неверный ключ даёт `VALIDATION_ERROR`. Регистр UUID нормализуется.

Синтаксическая проверка protobuf может идти до обработчика. Бизнес-проверки, лимиты и токены из тела (`refresh_token`, `registration_token`) выполняются **внутри callback**, после проверки ключа: повтор обязан получить сохранённый результат раньше этих отказов. `bank-bff` проверяет свою часть входа по контракту.

### 3. Обычный обработчик

```go
request, ok := idempotency.FromContext(ctx)
if !ok { return nil, finerr.InternalError() }
request.Hash, err = idempotency.HashProto(in)
if err != nil { return nil, finerr.InternalError() }

completion, cancel := idem.WithCompletion(ctx)
defer cancel() // AFTER db.Run has committed or rolled back
var result idempotency.Result
err = db.Run(completion, func(txCtx context.Context) error {
    var doErr error
    result, doErr = idem.Do(txCtx, request, func(effectCtx context.Context) (idempotency.Response, error) {
        // Business validation, limits and effect use repositories with effectCtx.
        out, domainErr := usecaseInTransaction(effectCtx, in)
        if domainErr != nil { return idempotency.Response{}, domainErr }
        body, marshalErr := proto.Marshal(out)
        return idempotency.Response{Body: body}, marshalErr
    })
    return doErr // NEVER return result.Err here: a cacheable error must commit.
})
if err != nil {
    if errors.Is(err, idempotency.ErrNotStored) { return nil, result.Err }
    if _, contract := finerr.ToREST(err); contract { return nil, err }
    return nil, finerr.InternalError()
}
if result.Err != nil { return nil, result.Err }
if err := grpc.SetHeader(ctx, metadata.MD(result.Response.Headers)); err != nil { return nil, finerr.InternalError() }
out := new(ResponseProto)
if err := proto.Unmarshal(result.Response.Body, out); err != nil { return nil, finerr.InternalError() }
return out, nil
```

Любая ошибка `Do` требует отката **всей** транзакции. `ErrNotStored` означает проверенный через `finerr.ToREST` несохраняемый контрактный исход в `result.Err`: после отката его возвращают клиенту. `result.Err` при успешном `Do` — сохраняемая ошибка: её возвращают только после успешного commit. При ошибке commit результат клиенту не возвращают как успешный. Неизвестная ошибка callback не попадает в `Result.Err`: возвращается `ErrStorage` с пустым результатом, сервис отвечает `finerr.InternalError()`; сырые ошибки SQL/сериализации наружу не передаются.

`WithCompletion` нужен **до** `db.Run`: транзакция и commit получают отвязанный контекст с собственным дедлайном, по умолчанию 30 с. До эффекта `Do` проверяет исходный контекст и использует его для работы с ключом; отмена до эффекта приводит к откату. После начала callback отмена клиента не мешает завершить эффект, запись ответа и commit. Библиотека не запускает горутину. Ограничение сервиса должно оставлять время на commit; длительность настраивается через `EffectTimeout`. Не используйте completion-контекст для внешних проверок до эффекта.

Первый запрос делает `INSERT … ON CONFLICT DO NOTHING RETURNING`; дубль ждёт на уникальном индексе. `lock_timeout` действует на операции с ключом и восстанавливается перед callback. По таймауту — `REQUEST_IN_PROGRESS` (`ABORTED`, `RetryInfo` с положительной задержкой); транзакция откатывается. Другое тело — `IDEMPOTENCY_CONFLICT` (`ALREADY_EXISTS`). Если первый запрос успел закоммититься, дубль возвращает его результат. Просроченный ключ выполняется заново; TTL готового ответа считается от его сохранения, по умолчанию 24 ч.

### 4. Хеш и сохраняемые исходы

`HashProto(in)` = deterministic protobuf marshal + SHA-256, предел 4 МиБ. После изменения схемы между деплоями возможен ложный конфликт — принятое ограничение контракта. Для `RetryAnswer` хешируйте предусмотренный контрактом `answer_id`; для загрузки файла используйте `HashFile(reader, maxBytes)` до транзакции: он потоковый, превышение предела — `FILE_TOO_LARGE`. Для streaming upload передайте хеш содержимого, рассчитанный сервисом при приёме файла; обрабатывайте ошибку чтения до эффекта.

Сохраняется protobuf успешного ответа либо полный gRPC status с деталями от `finerr`. Сохраняемость ошибки определяется через `finerr.ToREST`: только 400/413/422. Обычные 401/403/404/409/429/5xx, неизвестная или неполная ошибка откатывают транзакцию с ключом и эффектом. Успешный callback соответствует 2xx; HTTP-статус в библиотеку не передаётся. Максимум тела 4 МиБ, заголовков 64, значений на заголовок 64 и 16 КиБ суммарно; недопустимы `grpc-*`, неверные имена и CR/LF. Передавайте только прикладную metadata, не транспортные заголовки.

**Исключения StartPhoneVerification:**

- Когда номер занят, callback возвращает `finerr.RateLimited(retryAt)` и `Response{DuplicateRetryAt: retryAt}`. Только этот метод может сохранить такой 429 до абсолютного `retry_at`. Лимит списывается выполнением, а не повтором. Обычный отказ по лимиту этого поля не ставит и не сохраняется. При воспроизведении ошибка заново создаётся через `finerr.RateLimited(retryAt)`, поэтому `RetryInfo` пересчитывается.
- Сохраняйте `expires_at` в protobuf, а `expires_in` вычисляйте после `Do` **при каждой отправке**, включая повтор: `max(0, floor(expires_at-now))`. Пример проверяет `usage_test.go`.

Для `RefreshTokens` конструктор требует TTL ровно 60 с и `Encrypt: true`. AES-GCM шифрует весь сохранённый исход, включая заголовки; AAD связывает его со scope, методом и ключом. Храните ключ сервиса в секрете, сохраняйте прежний ключ до истечения зашифрованных записей. Этот API использует один ключ: смена ключа раньше срока сделает старые ответы нечитаемыми. Ошибка расшифровки закрывает запрос без повторного эффекта. `Register` и `StartPhoneVerification` также возвращают секреты: для них конструктор требует шифрование, TTL остаётся 24 ч. Если другие ответы содержат секреты, включите шифрование для них тоже. Библиотека не пишет ключи, ответы или SQL в логи; ошибки хранилища скрыты за `ErrStorage`.

### 5. AskQuestion: резерв перед проверкой согласия

1. В короткой транзакции сервиса вызовите `Reserve(ctx, request)`, затем commit. Если `result.Replayed`, верните результат после commit; token при этом нулевой. Ошибка требует rollback.
2. С полученным token, **вне транзакции**, проверьте согласие через `GetAssistantConsentStatus` на primary `user`. Используйте обычный контекст запроса с дедлайном unary 5 с.
3. Создайте `WithCompletion(ctx)` и в новой транзакции вызовите `DoReserved(txCtx, token, callback)`. Callback атомарно списывает лимит и создаёт вопрос/ответ; результат обрабатывается как у `Do`. Fencing проверяется перед callback и при записи ответа: совпадение `reservation_id`, срок строго больше текущего времени. Потерянный резерв не может закоммитить эффект.
4. После commit запускайте генерацию; сетевой вызов LLM не входит в транзакцию.
5. При любой ошибке до commit сначала откатите транзакцию, затем в **новой ограниченной по времени транзакции** вызовите `Release(ctx, token)`. Для очистки после отмены используйте `context.WithTimeout(context.WithoutCancel(ctx), ...)`. Это касается и отказа согласия, и лимита, и ошибки БД. Запоздалый Release удаляет только собственный незавершённый резерв. При падении процесса резерв истекает сам. Чтобы не открывать пустую cleanup-транзакцию на replay/дубле, перед `db.Run` можно проверить `token == (idempotency.Reservation{})` и пропустить очистку. Нулевой token (ошибка `Reserve` или replay) для `Release` — безопасный no-op; отложенную очистку можно ставить до вызова `Reserve` без доступа к внутренним полям token.

TTL резерва — 15 с по контракту, не TTL ответа. Дубль с тем же телом получает остаток TTL в `RetryInfo`; с другим — конфликт. После завершения — сохранённый ответ. Истёкший резерв перезанимается новой попыткой. Если при `DoReserved` другая попытка уже завершилась, возвращается её ответ; при незавершённом/просроченном/снятом резерве — `REQUEST_IN_PROGRESS` и откат. При истечении готового ответа нужна новая попытка через `Reserve`.

### 6. Периодическое задание и удаление пользователя

Зарегистрируйте задание сервиса (например, раз в минуту). В каждой короткой транзакции primary вызывайте `Cleanup(ctx, 1000)`, коммитьте пачку и продолжайте, пока число удалённых строк равно размеру пачки. Проверяйте отмену и завершайте задание с контекстом сервиса. Допустимый batch 1–10000, блокированные строки пропускаются через `SKIP LOCKED`. Порог — `expires_at <= Clock()`; время из часов сервиса, не SQL `now()`.

Проверка tombstone должна предшествовать чтению ключа: удалённый пользователь не получает старый ответ. В `PurgeUserData` сервис удаляет `idempotency_keys WHERE scope=$1` вместе с прочими данными пользователя; no-user scope нельзя удалять целиком при удалении одного пользователя. Библиотека не знает схемы сервиса и не выполняет purge сама.

### 7. Чек-лист нового эндпоинта

- OpenAPI требует `Idempotency-Key`, ответы 409 и проходит проверку FIN-77; RPC есть в contracts.
- Метод добавлен в точный allowlist `Methods`; scope берётся из проверенной идентичности.
- Хеш покрывает всё логическое тело по контракту; нулевой незаполненный хеш отклоняется; upload хешируется потоково.
- Выбран обычный `Do` либо резерв перед внешней проверкой. Все записи идут через `Tx(ctx)` на primary, транзакцией владеет сервис.
- Ошибки маппятся через `finerr`; сохраняемая ошибка коммитится, несохраняемая откатывается.
- `WithCompletion` окружает транзакцию и commit, а не только callback; внешних вызовов внутри callback нет.
- Секреты зашифрованы; TTL настроен; очистка и purge подключены; заголовки прикладные и ограничены.
- Сервисные API-тесты проверяют повтор, конфликт, параллельность, отмену, чужой scope, tombstone и ошибки метода.

## Метрики

`finassist_idempotency_requests_total{endpoint,outcome}`: `new`, `replay`, `conflict`, `in_progress`. `endpoint` — только метод из конфигурации; пользователь и ключ в метки не попадают. Счётчики фиксируют решение библиотеки до внешнего commit, поэтому не являются числом закоммиченных эффектов. Отслеживайте рост конфликтов/ожиданий и ошибки задания очистки; пороги задаёт сервис.

## Раскладка и проверки

`types.go`, `context.go`, `engine.go`, `storage.go`, `codec.go` — API, metadata/хеш, выполнение, PostgreSQL и сохранённые исходы. `migrations/` — embedded источник; `internal/pgtest/` — тестовая БД и up → down → up. `*_test.go` — unit, реальный PostgreSQL и usage gRPC; `example_test.go` — компилируемые примеры. Правила — [AGENTS.md](AGENTS.md), версии — [docs/releasing.md](docs/releasing.md), изменения — [CHANGELOG.md](CHANGELOG.md).

```sh
make test  # PostgreSQL 18 via Docker, or TEST_DATABASE_URL; vet, race, scripts, changelog
make lint
make cover # coverage of changed lines against origin/main
make api   # exported API versus latest tag
```

Все проверки в окружении агента запускаются с ограничением времени процесса. Без Docker/`TEST_DATABASE_URL` интеграционные тесты локально пропускаются, а в CI падают; пропуск не подтверждает работу с БД. CI также проверяет неизменность прежних миграций и squawk для новых. Минимум покрытия изменённых исполняемых строк — 80%; сценарии важнее процента.

### Общее покрытие и изменённые строки

CI публикует разделы «Покрытие проекта» и «Покрытие изменённых строк» в сводке запуска и одном обновляемом комментарии PR. Оба раздела используют один профиль корневого Go-модуля, без повторного запуска тестов; на push сохраняется общее покрытие. Комментарий для PR из fork не записывается. При ошибке построения или пустом файле отчёта последний корректный комментарий сохраняется.

Метрика — уникальные строки внутри блоков Go-профиля с ненулевым числом statements, а не процент statements из `go tool cover`. `_test.go` и `testdata/` исключены; сгенерированный production-код включён на тех же условиях. Повторные блоки разных тестовых бинарей объединяются: достаточно одного выполнения. Без исполняемых строк отчёт сообщает «Нет данных», а не 100 %. Цель **80 %** относится только к изменённым строкам и проверяется на ревью; общий процент её не заменяет.
