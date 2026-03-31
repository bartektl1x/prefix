SourceTableReference = Annotated[
    str,
    StringConstraints(
        min_length=1,
        strip_whitespace=True,
        pattern=r"^(\{[a-zA-Z_]\w*\}|[a-zA-Z_]\w*)(\.[a-zA-Z_]\w*)*$",
    ),
]
