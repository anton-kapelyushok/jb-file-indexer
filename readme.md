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

* watch root1 root2 -root3 // следить за каталогами root1, root2, игнорируя файлы в root3
* status // показать текущий статус
* find [term] // найти [term] в каталогах указанных в команде watch
* pollstatus // выводит статус каждую секунду
* exit

Пример исполнения:

````
Enter command (watch/find/status/exit/pollstatus)
watch .

Enter command (watch/find/status/exit/pollstatus)
find main()

searching, press any key to cancel
main(): C:\Users\Anton\projects\jb-file-indexer-1\readme.md:80
main(): C:\Users\Anton\projects\jb-file-indexer-1\app\src\main\kotlin\home\pathfinder\app\App.kt:11

Total 2 entries
Found in 28ms

Enter command (watch/find/status/exit/pollstatus)
exit

````

## Реализация

### FileIndexer

Менеджит запросы пользователя, индекс и вотчеры, обеспечивает коммуникацию между ними.

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

По мотивам индекса в Apache Lucene (https://www.youtube.com/watch?v=T5RmMNDR5XI)

Идея следующая: индекс разделен на несколько иммутабельных сегментов (Segment)

При добавлении документа создается новый сегмент.

При удалении просто помечаем документ как удаленный.

Обновление документа = удаление + обновление.

Если сегментов становится слишком много, специально обученный воркер сливает их в один.

При поиске текущий набор сегментов копируется и сам поиск осуществляется уже по этой копии.

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

`documents`: отсортированный список документов в индексе, `documentsState[i]` - состояние i го документа (удален или
нет)

`termData` + `termOffsets` - отсортированные термы в бинарном виде.   
`term[i] = String(termData.copyOfRange(termOffsets[i], termOffsets[i+1]))`

`dataTermIds[i]`, `dataDocIds[i]`, `dataTermData[i]` - данные об i-том вхождении терма. Вхождения отсортированы в этом
же порядке  
`dataTermIds[i]`  указывает на `termOffsets`, `dataDocIds` указывает на `documents`

### RootWatcher

В качестве file watcher библиотеки я рассматривал несколько вариантов:

* дефолтный - не подошел, так как для MacOS используется PollingWatcherService, который работает отвратительно медленно
* Idea filewatcher - показался, во-первых, сложным в интеграции, во-вторых, нечестным
* `io.methvin:directory-watcher` - остановился на этом, так как реализует нормальный WatcherService для MacOS.

Из недостатков этого вотчера - поведение отличается на разных платформах. Например, на Windows не получается смотреть за
изменениями одного файла. Так же этот filewatcher смотрит директории исключительно рекурсивно.

У `DirectoryWatcher` из этой библиотеки есть два долгих блокирующих вызова - `build()` и `watch()` и ему можно
передать `listener` файловых эвентов.

Класс `RootWatcher` - неблокирующая обертка над этим вотчером, реализованная в виде актора.

При запуске отправляются события на добавление файлов, которыми заведует вотчер.  
При завершении отправляются события на удаление всех файлов.

`FileIndexer` отслеживает состояние вотчера с помощью `RootWatcherState`.

