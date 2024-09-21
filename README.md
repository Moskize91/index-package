# index-package

# 安装

需要 Python 环境，执行如下命令创建 Python 的 `venv` 环境。

```shell
$ python -m venv .venv
```

此后切换到该 Python 环境。对于 Linux / MacOS 而言执行如下命令。

```shell
$ . .venv/bin/activate
```

对于 Windows 而言执行如下命令。

```shell
$ .venv\Scripts\activate.bat
```

此后安装依赖包，执行如下命令。

```shell
$ pip install -r requirements.txt
```

其中 Spacy 依赖的中英文模型，通过如下命令安装。

```shell
$ python -m spacy download en_core_web_sm
$ python -m spacy download zh_core_web_sm
```