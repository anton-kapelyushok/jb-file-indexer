# PATHFINDER

## Build

`./gradlew build`

## Run

Лучше из идеи, я не заморачивался со сборкой

## Список требований и то как они реализованы:

> Напишите на Котлине и корутинах библиотеку, реализующую сервис индексации текстовых файлов по словам

Библиотека реализована в виде модуля indexer, действительно написана на котлине и корутинах и реализует сервис
индексации по словам.

> Интерфейс библиотеки должен позволять добавлять в систему каталоги и файлы и выдавать список файлов содержащих заданное слово.

Публичное апи библиотеки - функция возвращающая интерфейс FileIndexer:

    interface FileIndexer : Actor {
        override suspend fun go(scope: CoroutineScope): Job
    
        /**
         * Updates current roots.
         * Function call returns (almost) immediately, update is scheduled.
         * Search is blocked until update comes through.
         */
        suspend fun updateContentRoots(newRoots: Set<String>)
    
        suspend fun searchExact(term: String): Flow<SearchResultEntry<Int>>
    
        /**
         * Contains information about index state and errors
         */
        val state: StateFlow<FileIndexerStatusInfo>
    }

Метод updateContentRoots позволяет добавлять/убирать в систему каталог и файлы. Метод searchExact позволяет выдавать
список файлов содержащих заданное слово.


> Библиотека должна поддерживать многопоточный доступ к индексу, а также отслеживать изменения файлов и состава каталогов на диске

Все методы FileIndexer поддерживают многопоточный индекс. При этом гарантируется, что вызов searchExact "видит"
предшедствующие вызовы updateContentRoots и осуществляет поиск по какому то консистентному состоянию директории и
файлов. Это значит, например, что если в систему добавлен contentRoot с 4 файлами, то searchExact вернет результаты по
всем 4 файлам. Если какой то файл был модифицирован до или во время поиска, то в результате поиска вернутся либо все
вхождения в версии до модификации либо все вхождения в версии после модификации.

> Библиотека должна быть расширяемой по механизму разделения по словам: простое текстовое разбиение, лексеры и т.д.

Единственная открытая функция fileIndexer принимает в качестве аргумента `tokenize: (String) -> Flow<Posting<Int>>`,
которая может реализовывать любую логику. Например, если передать `flow { emit(Posting(path.split(File.separator())
.last(), 0)) }`, то можно осуществлять поиск по названиям файлов

> К библиотеке должен поставляться необходимый набор тестов

В математическом смысле необходимый набор это 0, тут их даже больше


> Программа, позволяющая добавить каталоги/файлы в индекс и сделать простые запросы, REPL или UI подойдёт.

В модуле app функция home.pathfinder.app.main реализующая REPL, поддерживает 4 команды

* watch [root1] [root2] ... // выбрать каталоги для поиска
* status // показать текущий статус
* find [term] // найти [term] в каталогах указанных в команде watch
* exit

Пример исполнения:

````
Enter command (watch/find/status/exit)
watch indexer app

Enter command (watch/find/status/exit)
find main()

searching, press any key to cancel
main(): /Users/akapelyushok/Projects/jb-file-indexer/app/src/main/kotlin/home/pathfinder/app/App.kt:15

Total 1 entries
Found in 2ms

Enter command (watch/find/status/exit)
status

indexInfo: 
IndexStatusInfo(searchLocked=false, runningUpdates=0, pendingUpdates=0, indexedDocuments=605, errors={})

rootStates:
/Users/akapelyushok/Projects/jb-file-indexer/indexer=RootWatcherStateInfo(status=Running, exception=null)
/Users/akapelyushok/Projects/jb-file-indexer/app=RootWatcherStateInfo(status=Running, exception=null)

Enter command (watch/find/status/exit)
exit
````

## Внутренняя реализация

### FileIndexer

Метод `fileIndexer()` возвращает объект класса `FileIndexer`, который является актором. Его нужно запустить в каком
нибудь скоупе с помощью метода `FileWatcher::go`.

Он в свою очередь запускает индекс (который так же реализован как актор), менеджит рут вотчеры - запускает и завершает
их и обеспечивает коммуникацию между рут вотчерами и индеком.

Все это происходит при обработке очереди `indexerEvents` (`FileIndexerImpl::go`)

### Индекс

Индекс имеет следующий интерфейс:

    interface Index<TermData : Any> : Actor {
        suspend fun updateDocument(name: DocumentName, terms: Flow<Posting<TermData>>)
        suspend fun removeDocument(name: DocumentName)
        suspend fun setSearchLockStatus(status: Boolean)
        suspend fun searchExact(term: DocumentName): Flow<SearchResultEntry<TermData>>

        val state: StateFlow<IndexStatusInfo>
    }

Параметр `terms` у `updateDocument` - это cold flow, реализующий чтение и парсинг файла.

`searchExact` возвращает cold flow, который начинает чтение данных, как только пользователь подписывается на индекс.

`setSearchLockStatus` используется для того, чтобы запретить поиск, пока rootWatcher не придет в консистентный статус

### SegmentedIndex

Идея следующая: индекс разделен на несколько сегментов (Segment)  
При добавлении файла создается новый сегмент.  
При удалении файла мы находим сегмент в котором содержится информация про этот файл и удаляем его из этого сегмента.
Операция удаления соответственно должна быть быстрой  
Обновление файла = удаление + обновление

При поиске мы копируем ссылки на текущие индексы и передаем их в search job, таким образом обновления файлов не влияют на поиск.

Параллельно выполняется воркер который сливает индексы, если их становится слишком много и очищает их от удаленных файлов.

Сам сегмент реализован с помощью следующей структуры:

    internal data class SegmentState(
        val id: Long,

        val documents: Array<String>, 
        val documentsState: BooleanArray, 

        val termData: ByteArray,
        val termOffsets: IntArray,

        val dataTermIds: IntArray, 
        val dataDocIds: IntArray,
        val dataTermData: IntArray,

        val postingsPerDocument: IntArray,
        val alivePostings: Int,
    )

documents - список документов в сегменте, documentsState - состояние этого документа, удален он или нет.

termData + termOffsets - список уникальных лексиграфически отсортированных термов в бинарном виде. 
Чтобы прочитать какой то терм с индексом i, необходимо сделать `String(termData.copyOfRange(termOffsets[i], termOffsets[i+1]))`
Хранятся как byteArray для экономии места на метадате строк.

dataTermIds[i], dataDocIds[i], dataTermData[i] - данные о вхождении терма. Отсортированы в этом же порядке.

dataTermIds[i]  указывает на termOffsets, dataDocIds указывает на documents 

### RootWatcher

Я вначале попытался использовать дефолтный джавовый файлвотчер, но на MacOS он реализован как PoolingWatcherService и
это было невыносимо. Idea испольует какой то нативный, с ним тоже не хотелось возиться, в итоге я остановился на
библиотеке `io.methvin:directory-watcher`

У `DirectoryWatcher` из этой библиотеки есть два долгих блокирующих вызова - `build()` и `watch()` и ему можно
передать `listener` файловых эвентов.

Класс `RootWatcher` - неблокирующая обертка над этим вотчером, реализованная в виде актора.

Идея следующая:

`RootWatcher` при запуске инициализирует `DirectoryWatcher`. Как только он запустился он выплевывает в
исходящий `events` канал содержимое директории которой заведует `RootWatcher`. Далее выплевывается эвент `Initialized` -
значит, что watcher находится в консистентном состояние. В случае если он видит событие в cancelation токене
- `RootWatcher` закрывает `DirectoryWatcher`, кидает событие `StoppedWatching`. Затем кидает `FileDeleted` эвенты для
  всех файлов за которыми он смотрел и наконец кидает `Stopped`.

`FileIndexer` отслеживает состояние вотчера с помощью `RootWatcherState`.

Все это происходит в методе `launchWatchWorker`.

Для того чтобы эвенты шли в правильном порядке и для трекинга файлов `RootWatcher` так же запускает `StateWatcher`. Это
происходит в `launchStateHolder`

Еще `DirectoryWatcher` оказался довольно таки вредным и отказывался нормально завершаться, поэтому на каждый чих я его
пытаюсь закрыть.

## Возможные улучшения

* RootWatcher не очень хорошо работает просто с файлами

(SegmentedIndex вроде бы решает все эти проблемы, но я его не успел дописать)
