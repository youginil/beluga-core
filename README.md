# beluga-core

## Beluga

#### Dictionary

| Title           | Structure                                                                                                        |
| --------------- | ---------------------------------------------------------------------------------------------------------------- |
| Metadata length | (4B)                                                                                                             |
| Metadata        | {spec: u8, version: String, entry_num: u64, author: String, email: String, create_time: String, comment: String} |
| Nodes           | (node compressed by Deflate)...                                                                                  |
| Root Node       | (root offset 8B) (root size 4B)                                                                                  |

#### Node

| Title       | Structure                                                                                                                                               |
| ----------- | ------------------------------------------------------------------------------------------------------------------------------------------------------- |
| is leaf     | (1B) 0 - leaf; other - !leaf                                                                                                                            |
| Entry count | (4B)                                                                                                                                                    |
| Entris      | (key length 4B)(key) (leaf ? (value length 4B)(value) : None)...                                                                                        |
| Children    | !leaf : (child offset 8B)(child length 4B)...<br> leaf: (next sibling offset 8B)(next sibling length 4B)...<br>Offset of the last leaf node' child is 0 |

## Raw

#### Fields

## Name | Type

id | INTEGER
name | TEXT
text | TEXT
binary | BLOB

#### SQL

```
select * from word group by name having count(*) > 1;
```

## References

#### Mdict

- https://github.com/csarron/mdict-analysis
- https://github.com/zhansliu/writemdict/blob/master/fileformat.md
- https://github.com/fengdh/mdict-js
- https://github.com/ilius/pyglossary

#### Fulltext Index

- https://github.com/stanfordnlp/CoreNLP
- https://github.com/hankcs/HanLP
- https://github.com/nltk/nltk
- https://github.com/RaRe-Technologies/gensim
- https://github.com/explosion/spaCy
- https://github.com/keras-team/keras
- https://github.com/thunlp/THULAC
- https://github.com/yanyiwu/cppjieba
- @ https://github.com/fxsjy/jieba
- https://github.com/HIT-SCIR/ltp
- https://github.com/NLPchina/ansj_seg

#### Dictionary Online

- https://www.thesaurus.com/
- https://www.dictionary.com/
- https://dictionary.cambridge.org/
- https://www.collinsdictionary.com/
- https://www.macmillandictionary.com/
- https://www.ldoceonline.com/
- https://www.oxfordlearnersdictionaries.com/

## TODO

- Checksum
- Remove special chars and set @@@LINK=
- HTML validation, fix, prettify. `tidy-html5`, `cheerio`
- Convert resource id to `<a href="audio://a/b/c.mp3">`, `<img data-src="a/b/c.jpg">`
- Capture. `puppeteer`
- Performance monitor
- Tire tree
- "assist in" Merge(Result<assist>, Result<in>)
