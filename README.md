# FunPy
Support for construction of simulation and data processing workflows in terms of pure functions in Python.

**Fetures**
- *Persistent memoization of pure functions dependent on code.*
   E.g. you can develop a data processing script, memoize intermediate results automatically to files (persistent).
   Once input data or the implementation (code) of your functions change the results are automatically recomputed.
- Support for hashing of intermediate files. Usefull for more complex workflows.
- Dataclasses initialization from YAML files.

**Planed features**
- Distributed result cache based on REDIS.
- Synactic suggar enhancing functional programming:
  - function composition using  `@` operator
  - simplified lambda functions using argument placeholders

