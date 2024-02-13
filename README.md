# eggie_kv

<div>
<img 
    src="https://img.shields.io/github/license/Trinoooo/eggie_kv" 
    alt="license"
>
<a href="https://pkg.go.dev/github.com/Trinoooo/eggie_kv">
<img 
    src="https://img.shields.io/badge/reference-reference?logo=go&labelColor=gray&color=blue&link=https%3A%2F%2Fpkg.go.dev%2Fgithub.com%2FTrinoooo%2Feggie_kv" 
    alt="reference"
>
</a>
<a href="https://github.com/Trinoooo/eggie_kv/actions/workflows/test.yaml">
<img 
    src="https://github.com/Trinoooo/eggie_kv/actions/workflows/test.yaml/badge.svg" 
    alt="go test"
>
</a>
<a href="https://codecov.io/gh/Trinoooo/eggie_kv" > 
 <img src="https://codecov.io/gh/Trinoooo/eggie_kv/graph/badge.svg?token=A86TGA8XOE" alt="codecov"/> 
</a>
</div>

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

