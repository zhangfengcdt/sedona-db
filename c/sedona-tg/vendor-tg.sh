
TG_REF=efbb5b704d0df5ae3d23bf325683e0248ee9b6d8

curl -L https://github.com/tidwall/tg/raw/${TG_REF}/tg.c \
    -o src/tg/tg.c

curl -L https://github.com/tidwall/tg/raw/${TG_REF}/tg.h \
    -o src/tg/tg.h
