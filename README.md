# eggie_kv
<!-- ALL-CONTRIBUTORS-BADGE:START - Do not remove or modify this section -->
[![All Contributors](https://img.shields.io/badge/all_contributors-1-orange.svg?style=flat-square)](#contributors-)
<!-- ALL-CONTRIBUTORS-BADGE:END -->

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


## Contributors ✨

Thanks goes to these wonderful people ([emoji key](https://allcontributors.org/docs/en/emoji-key)):

<!-- ALL-CONTRIBUTORS-LIST:START - Do not remove or modify this section -->
<!-- prettier-ignore-start -->
<!-- markdownlint-disable -->
<table>
  <tbody>
    <tr>
      <td align="center" valign="top" width="14.28%"><a href="https://trinoooo.github.io/"><img src="https://avatars.githubusercontent.com/u/101500554?v=4?s=100" width="100px;" alt="Trino"/><br /><sub><b>Trino</b></sub></a><br /><a href="https://github.com/Trinoooo/eggie_kv/commits?author=Trinoooo" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/RocooHash"><img src="https://avatars.githubusercontent.com/u/53431133?v=4?s=100" width="100px;" alt="Liang Xuhao"/><br /><sub><b>Liang Xuhao</b></sub></a><br /><a href="https://github.com/Trinoooo/eggie_kv/commits?author=RocooHash" title="Code">💻</a></td>
    </tr>
  </tbody>
</table>

<!-- markdownlint-restore -->
<!-- prettier-ignore-end -->

<!-- ALL-CONTRIBUTORS-LIST:END -->

This project follows the [all-contributors](https://github.com/all-contributors/all-contributors) specification. Contributions of any kind welcome!