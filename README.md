# eggie_kv

## 模块间关系图
```text
.
├── LICENSE
├── README.md
├── consts
│   ├── common.go
│   └── error.go
├── core
│   ├── cli
│   │   └── cli.go
│   ├── eggie_kv_server
│   ├── kv
│   │   ├── batch.go
│   │   ├── channel.go
│   │   ├── data.go
│   │   ├── kv.go
│   │   ├── wal.go
│   │   └── wal_test.go
│   ├── main.go
│   └── server
│       ├── handler.go
│       ├── mw.go
│       └── server.go
├── doc
│   ├── README.md
│   ├── babel.config.js
│   ├── blog
│   │   ├── 2019-05-28-first-blog-post.md
│   │   ├── 2019-05-29-long-blog-post.md
│   │   ├── 2021-08-01-mdx-blog-post.mdx
│   │   ├── 2021-08-26-welcome
│   │   └── authors.yml
│   ├── build
│   │   ├── 404.html
│   │   ├── assets
│   │   ├── blog
│   │   ├── docs
│   │   ├── img
│   │   ├── index.html
│   │   ├── markdown-page
│   │   └── sitemap.xml
│   ├── docs
│   │   ├── intro.md
│   │   ├── tutorial-basics
│   │   └── tutorial-extras
│   ├── docusaurus.config.js
│   ├── package-lock.json
│   ├── package.json
│   ├── sidebars.js
│   ├── src
│   │   ├── components
│   │   ├── css
│   │   └── pages
│   └── static
│       └── img
├── go.mod
├── go.sum
├── interactive
│   ├── cli
│   │   ├── eggie_kv_client
│   │   └── main.go
│   └── client_sdk
├── makefile
├── test_data
│   ├── f1
│   ├── f2
│   └── f3
└── utils
    ├── chan.go
    ├── chan_test.go
    ├── file.go
    ├── file_test.go
    ├── lock.go
    ├── lock_test.go
    ├── panic.go
    └── panic_test.go

```

