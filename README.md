ParallelServer
==============

ParallelServer は Ruby の並列 TCP/IP サーバーを簡単に作ることが出来るライブラリです。

ParallelServer::Prefork
-----------------------

あらかじめ処理用のプロセスを生成しておき、接続毎にスレッドを生成して処理を実行します。

### 例

```ruby
require 'parallel_server/prefork'

pl = ParallelServer::Prefork.new(12345, max_processes: 100, max_idle: 100)
pl.start do |sock, addr|
  sock.puts 'Who are you?'
  name = sock.gets
  sock.puts "Hello, #{name}"
end
```

### ParallelServer::Prefork.new

* `ParallelServer::Prefork.new(port, opts={})`
* `ParallelServer::Prefork.new(host, port, opts={})`

#### host

待ち受けるIPアドレス。省略時はすべてのIPアドレスで待ち受けます。

#### port
待ち受けるTCPポート番号。

#### opts

ParallelServer::Prefork の動作を設定するパラメータ。

`:min_processes` :
最小プロセス数を指定します(デフォルト: 5)。

`:max_processes` :
最大プロセス数を指定します(デフォルト: 20)。

`:max_threads` :
1プロセスあたりの最大スレッド数を指定します(デフォルト: 1)。

`:standby_threads` :
空きスレッド数を指定します(デフォルト: 5)。
少なくともこの数だけの接続を受け付けられるようにプロセス数を調整します。

`:listen_backlog` :
待ち受けポートの listen backlog を指定します(デフォルト: nil)。
未指定時は何も設定しないので backlog の値はシステム依存です。

`:max_idle` :
指定秒数の間、クライアントからの新たな接続がないとプロセスを終了します(デフォルト: 10)。
生成後一度も接続されていないプロセスはこのパラメータの影響を受けません。
max_idle 経過後でもクライアントと接続中であればプロセスは終了しません。ただし `:min_processes`, `:max_processes` のカウント対象外です。

`:on_child_start` :
子プロセス起動時に*子プロセス側*で実行される処理を Proc で指定します。Proc 実行時の引数はありません。

`:on_child_exit` :
子プロセス終了時に*親プロセス側*で実行される処理を Proc で指定します。Proc 実行時の引数はプロセスID(Integer)と終了ステータス(Process::Status)です。

### #start

* `start{|sock, addr| ...}`

待ち受けを開始します。クライアントから接続する毎にスレッドを生成して、ブロックを実行します。
ブロックパラメータは Socket と Addrinfo です。

### #reload

* `reload(port, opts={})`
* `reload(host, port, opts={})`

引数の形式は new と同じです。

start 後にパラメータを変更したい場合に使用します。

### #stop

* `stop`

start を終了します。クライアントと接続中の子プロセスは接続が切断されるまで終了しません。

### #stop!

* `stop!`

start を終了します。子プロセスがクライアントと接続中でも SIGTERM で終了させます。
