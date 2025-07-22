
TG_REF=e136401cd6870008eb7d12a2a165fdc27d18d5ef

curl -L https://github.com/tidwall/tg/raw/${TG_REF}/tg.c \
    -o src/tg/tg.c

curl -L https://github.com/tidwall/tg/raw/${TG_REF}/tg.h \
    -o src/tg/tg.h
