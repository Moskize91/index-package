# index-package

## 开发环境

### 安装 levelDB （MacOS）

```shell
brew install leveldb
```

对于 M1 芯片，在安装 `requirements.txt` 内容前，需要配置如下环境变量。

```shell
export LIBRARY_PATH="$LIBRARY_PATH:$(brew --prefix)/lib"
export CPATH="$CPATH:$(brew --prefix)/include"
```

换而言之，在下一小节中 `python -m venv .venv` 命令之前要准备好这些环境变量。
该内容参考 [issue#100](https://github.com/wbolster/plyvel/issues/100#issuecomment-1162625134)。

### python 环境

创建 .env 环境。

```shell
python -m venv .venv
```

在 terminal 中使用该环境。

```shell
. .venv/bin/activate
```