# Go Language Reference

Based on [A Tour of Go](https://go.dev/tour/) and the Go Language Specification.

---

## 1. Lexical Elements

### Tokens

- **Identifiers**: letters `_` or Unicode letter, followed by zero or more letters or digits.
- **Keywords** (25, cannot be used as identifiers):

```
break        default      func         interface    select
case         defer        go           map          struct
chan         else         goto         package      switch
const        fallthrough  if           range        type
continue     for          import       return       var
```

- **Predeclared identifiers** (not keywords, can be shadowed):

```
Types:     bool byte complex64 complex128 error float32 float64
           int int8 int16 int32 int64 rune string
           uint uint8 uint16 uint32 uint64 uintptr

Constants: true false iota

Zero:      nil

Functions: append cap close complex copy delete imag len
           make new panic print println real recover
```

### Operators

```
Arithmetic:    +  -  *  /  %  &  |  ^  <<  >>  &^
Comparison:    ==  !=  <  <=  >  >=
Logical:       &&  ||  !
Assignment:    =  +=  -=  *=  /=  %=  &=  |=  ^=  <<=  >>=  &^=
Increment:     ++  --  (statements, not expressions)
Send:          <-
Address:       &  *
Ellipsis:      ...
```

### Literals

```
Integer:       42  0xFF  0o77  0b1010
Floating:      3.14  1e-9  0x1p-3
Imaginary:     1i  0.5i
Rune:          'a'  '\n'  '\x41'  '\u0041'  '\U00000041'
String:        "hello"  "line1\nline2"
Raw string:    `raw\nstring`  (no escape processing)
```

### Semicolons

Semicolons are **automatically inserted** by the lexer (not in source):
- After an identifier, number, string, or keyword `break/continue/fallthrough/return/++/--/)/}` followed by a newline.
- Never put opening `{` on a new line.

---

## 2. Packages, Imports, and Files

### File Structure

A Go file starts with `package`, then `import`s (optional), then top-level declarations.

Syntax:
```text
PackageClause  = "package" identifier .
ImportDecl     = "import" ( ImportSpec | "(" { ImportSpec } ")" ) .
ImportSpec     = [ "." | PackageName ] ImportPath .
ImportPath     = string_lit .
```

```go
package main                  // package clause, required first line

import "fmt"                  // single import
import (                      // grouped import
    "fmt"
    "math/rand"
    f "fmt"                   // with alias
    . "math"                  // dot import (use directly: Pi)
    _ "net/http/pprof"        // blank import (init only)
)

// top-level declarations follow
```

**Rules:**
- A Go program is a collection of packages linked together.
- The `main` package with `func main()` is the entry point of an executable.
- Every `.go` file begins with a package declaration.
- All files in the same directory must belong to the same package.
- Import paths are strings: `"fmt"`, `"net/http"`, `"github.com/foo/bar"`.
- If an imported package is not used, compilation fails.
- The blank identifier `_` as the package name imports only for side effects (`init()` functions).

### Visibility

```text
Name → exported if first character is Unicode uppercase
       package-private (unexported) otherwise
```

- Exported names are accessible from other packages: `fmt.Println`.
- Unexported names are only visible within the same package.

---

## 3. Declarations

### var — Variable Declaration

Syntax:
```text
VarDecl    = "var" ( VarSpec | "(" { VarSpec } ")" ) .
VarSpec    = identifier [ "=" expression ]
           | identifier { "," identifier } Type [ "=" expression_list ] .
```

```go
var i int                       // zero value (0)
var i = 42                      // type inference
var i int = 42                  // explicit type
var x, y int = 1, 2             // multiple with single type
var x, y = 1, "two"             // multiple with mixed types
var (                           // grouped declaration
    a = 1
    b = 2
)
```

**Rules:**
- Every declared variable must be **used** (read at least once), or compilation fails.
- Package-level `var` can only use `=` (not `:=`).
- Variables without initializer get the **zero value**.

### := — Short Variable Declaration

Syntax:
```text
ShortVarDecl = identifier_list ":=" expression_list .
```

```go
i := 42                         // inside functions only
x, y := 1, "two"                // multiple
a, b := f()                     // from multi-return function
a, b = b, a                     // swap (:= not used — no new variables)
a, c := 1, 2                    // at least one new variable required
```

**Rules:**
- `:=` must declare at least one **new** variable in the current scope.
- Only valid inside function bodies (not at package level).
- Cannot be used with `const`.

### type — Type Declaration

Syntax:
```text
TypeDecl   = "type" ( TypeSpec | "(" { TypeSpec } ")" ) .
TypeSpec   = AliasDecl | TypeDef .
AliasDecl  = identifier "=" Type .
TypeDef    = identifier Type .
```

```go
type MyInt int                    // new named type (distinct from int)
type MyInt = int                  // alias (MyInt and int are interchangeable)
type (
    Point struct{ X, Y float64 }
    Reader io.Reader
)
```

**Rules:**
- A type definition creates a new, distinct type (even if the underlying type is the same).
- A type alias makes the name an exact synonym — no conversion needed.
- Methods can only be defined on types in the same package.

### const — Constant Declaration

Syntax:
```text
ConstDecl  = "const" ( ConstSpec | "(" { ConstSpec } ")" ) .
ConstSpec  = identifier_list [ [ Type ] "=" expression_list ] .
```

```go
const Pi = 3.14
const Pi float64 = 3.14                  // typed constant
const (
    a = 1
    b                                   // = 1  (repeats previous expression)
    c = 2
)
const (
    _  = iota                           // 0, discarded
    KB = 1 << (10 * iota)               // 1 << 10
    MB                                  // 1 << 20
    GB                                  // 1 << 30
)
```

**Rules:**
- Constants must be **compile-time evaluable** (numbers, strings, booleans, built-in functions `len`/`cap`/`real`/`imag`/`complex` on constant arguments).
- Untyped constants (no explicit type) can be implicitly converted when assigned.
- Typed constants require explicit conversion.
- `iota` resets to 0 in each `const` block and increments by 1 per line.

### The Blank Identifier `_`

Used to discard values (never causes "unused" error):
```go
for _, v := range slice { }
v, _ := strconv.Atoi("42")
import _ "net/http/pprof"         // call init() only
var _ io.Reader = &MyType{}       // compile-time interface check
```

---

## 4. Types

### Type Syntax (simplified grammar)

```text
Type        = TypeName | TypeLit | "(" Type ")" .
TypeName    = identifier | qualified_ident .
TypeLit     = ArrayType | StructType | PointerType | FunctionType
            | InterfaceType | SliceType | MapType | ChannelType .
```

### Basic Types

| Type | Zero Value | Description |
|---|---|---|
| `bool` | `false` | |
| `string` | `""` | immutable byte sequence (UTF-8 conventionally) |
| `int` / `uint` | `0` | platform-sized (32 or 64 bit) |
| `int8` / `uint8` (byte) | `0` | 8-bit |
| `int16` / `uint16` | `0` | 16-bit |
| `int32` / `uint32` (rune = int32) | `0` | 32-bit |
| `int64` / `uint64` | `0` | 64-bit |
| `uintptr` | `0` | large enough to hold pointer bits |
| `float32` / `float64` | `0.0` | IEEE 754 |
| `complex64` / `complex128` | `0+0i` | |
| `byte` | `0` | alias for uint8 |
| `rune` | `0` | alias for int32, for Unicode code point |
| `error` | `nil` | built-in interface |
| `any` | `nil` | alias for `interface{}` (Go 1.18+) |

**Conversion rules:**
- Go has **no implicit numeric conversions** — all must be explicit: `float64(i)`.
- Numeric types can convert between each other (may truncate/crop).
- `string(i)` converts integer to rune string (not decimal representation).
- `[]byte(s)` and `string([]byte)` convert between string and byte slice.

### Pointer Types

Syntax:
```text
PointerType = "*" Type .
```

```go
var p *int                     // nil pointer, zero value is nil
p = new(int)                   // *int pointing to zero-value int
p = &x                         // address of existing variable
```

**Rules:**
- Zero value is `nil`.
- **No pointer arithmetic** (unlike C).
- `&x` produces the address of a variable.
- `*p` dereferences a pointer.
- `new(T)` allocates a zero-valued `T` and returns `*T`.

### Struct Types

Syntax:
```text
StructType    = "struct" "{" { FieldDecl } "}" .
FieldDecl     = (identifier_list Type | EmbeddedField) [ Tag ] .
EmbeddedField = [ "*" ] TypeName .
Tag           = string_lit .
```

```go
type Person struct {
    Name string
    Age  int
    *Address                      // embedded (promoted fields)
    json.RawMessage               // embedded from another package
    phone string `json:"phone,omitempty"`  // struct tag
}
```

**Rules:**
- Fields are accessed with `.` notation.
- Embedded (anonymous) fields promote their methods and fields to the outer struct.
- Struct tags are metadata strings accessed via `reflect`.
- Zero value: each field at its zero value.
- Comparing structs: two structs are comparable only if all their fields are comparable (no slices/maps — use `reflect.DeepEqual`).

### Array Types

Syntax:
```text
ArrayType = "[" array_length "]" Type .
```

```go
var a [5]int                     // [0 0 0 0 0]
b := [3]int{1, 2, 3}
c := [...]int{1, 2, 3}           // compiler counts: [3]int
d := [5]int{1, 2}                // [1 2 0 0 0]
e := [5]int{2: 10, 4: 30}        // [0 0 10 0 30]  (indexed init)
```

**Rules:**
- Length is part of the **type** — `[3]int` and `[4]int` are different types.
- Arrays are **values** — assignment copies the entire array.
- Pass by value (expensive for large arrays; prefer slices).
- Access with `arr[i]`, bounds-checked at runtime.

### Slice Types

Syntax:
```text
SliceType = "[" "]" Type .
```

```go
var s []int                      // nil, len=0
s = []int{1, 2, 3}              // slice literal
s = make([]int, len)             // zeroed slice
s = make([]int, len, cap)        // with explicit capacity
s = arr[1:4]                     // from array or other slice
s = arr[1:4:5]                   // slice with max capacity (cap=5-1)
```

**Built-in slice functions:**

| Function | Signature | Behavior |
|---|---|---|
| `len` | `(s []T) int` | number of elements |
| `cap` | `(s []T) int` | capacity of underlying array |
| `append` | `(s []T, vs ...T) []T` | append elements, may reallocate |
| `copy` | `(dst, src []T) int` | copies min(len(dst), len(src)) elements |
| `make` | `([]T, len, ...cap) []T` | create a new slice |

**Rules:**
- Zero value is `nil` (length and capacity both 0).
- Slice is a descriptor: `(pointer, length, capacity)` into a backing array.
- `append` may reallocate and return a new slice — always assign: `s = append(s, v)`.
- `nil` slice is safe with `append`, `len`, `cap`.
- Slicing beyond `cap` panics; slicing beyond `len` extends the slice (up to `cap`).

### Map Types

Syntax:
```text
MapType = "map" "[" Type "]" Type .
```

```go
var m map[string]int             // nil, reading OK, writing panics
m = make(map[string]int)
m = make(map[string]int, 100)    // with initial capacity hint
m = map[string]int{"a": 1}       // map literal

v := m["key"]                    // zero if key missing
v, ok := m["key"]                // ok == false if key missing
delete(m, "key")
```

**Rules:**
- Key type must be **comparable** (`==` and `!=` defined).
- Zero value is `nil` — reading from nil map returns zero value, **writing to nil map panics**.
- Maps are **reference types** — assignment shares the underlying data (no copy).
- Iteration order is **randomized** — never depend on it.
- Safe to modify a map during iteration (add/delete).
- Passing a map to a function shares the map — modifications affect caller.

### Function Types

Syntax:
```text
FunctionType   = "func" Signature .
Signature      = Parameters [ Result ] .
Result         = Parameters | Type .
Parameters     = "(" [ ParameterList [ "," ] ] ")" .
ParameterList  = ParameterDecl { "," ParameterDecl } .
ParameterDecl  = [ identifier_list ] [ "..." ] Type .
```

```go
type F func(int, string) (bool, error)
type H func(int, ...string)       // variadic
```

### Interface Types

Syntax:
```text
InterfaceType = "interface" "{" { MethodElem | TypeElem } "}" .
MethodElem    = MethodName Signature .
TypeElem      = TypeTerm { "|" TypeTerm } .
TypeTerm      = Type | "~" Type .
```

```go
// method-only interface
type Stringer interface {
    String() string
}

// empty interface = any value
var v interface{}
var v any                         // same as above (Go 1.18+)

// type constraint (Go 1.18+)
type Number interface {
    int | int64 | float64         // union
    ~int | ~int64                 // any type with underlying int/int64
    String() string               // mix of methods and type terms
}
```

**Rules:**
- Interface satisfaction is **implicit** — a type satisfies an interface by implementing all its methods (no `implements` keyword).
- `any` is an alias for `interface{}`.
- Go 1.18+ interfaces can contain type terms for use as **type constraints** (generics).
- Pre-Go 1.18 compatible interfaces (no type terms) can be used as variable types.

### Channel Types

Syntax:
```text
ChannelType = ( "chan" | "chan" "<-" | "<-" "chan" ) Type .
```

```go
ch := make(chan int)              // bidirectional, unbuffered
ch := make(chan int, 100)         // bidirectional, buffered
var r <-chan int = ch             // receive-only
var s chan<- int = ch             // send-only
```

**Direction syntax:**
- `chan T` — send and receive.
- `chan<- T` — send only (prevents receiving).
- `<-chan T` — receive only (prevents sending).

**Rules:**
- Zero value is `nil`.
- `nil` channel: send blocks forever, receive blocks forever, `close(nil)` panics.
- Closed channel: receive returns zero value + `false`, send panics.

---

## 5. Control Flow

### if

Syntax:
```text
IfStmt = "if" [ SimpleStmt ";" ] Expression Block [ "else" ( IfStmt | Block ) ] .
```

```go
if x > 0 {
    return x
}

if v := math.Pow(x, n); v < limit {
    return v
} else {
    return limit
}
```

**Rules:**
- Condition must be a `bool` expression (no truthy/falsy).
- The optional SimpleStmt executes before the condition is evaluated.
- Variables declared in the SimpleStmt are scoped to the **entire** `if-else` chain.
- Parentheses around the condition are **not** used (unlike C/Java).

### switch

Syntax:
```text
SwitchStmt       = ( ExprSwitchStmt | TypeSwitchStmt ) .
ExprSwitchStmt   = "switch" [ SimpleStmt ";" ] [ Expression ]
                   "{" { ExprCaseClause } "}" .
ExprCaseClause   = ExprSwitchCase ":" StatementList .
ExprSwitchCase   = "case" ( ExpressionList | "default" ) .
TypeSwitchStmt   = "switch" [ SimpleStmt ";" ] TypeSwitchGuard
                   "{" { TypeCaseClause } "}" .
TypeSwitchGuard  = [ identifier ":=" ] PrimaryExpr "." "(" "type" ")" .
TypeCaseClause   = TypeSwitchCase ":" StatementList .
TypeSwitchCase   = "case" ( TypeList | "default" ) .
```

```go
// expression switch
switch os := runtime.GOOS; os {
case "darwin":
    fmt.Println("macOS")
case "linux":
    fmt.Println("Linux")
default:
    fmt.Println(os)
}

// tagless switch (replaces if-else-if chain)
switch {
case x < 0:
    return -x
case x == 0:
    return 0
default:
    return x
}

// type switch
var v interface{} = 42
switch t := v.(type) {
case int:
    fmt.Println("int:", t)
case string:
    fmt.Println("string:", t)
case nil:
    fmt.Println("nil")
default:
    fmt.Printf("unknown type %T\n", t)
}
```

**Rules:**
- No `break` needed — cases do **not** fall through.
- Use `fallthrough` to explicitly fall through to the **next** case (it falls through unconditionally — the next case is not evaluated).
- `default` can appear anywhere in the switch.
- Multiple values per case: `case 1, 2, 3:`.
- The SimpleStmt is scoped to the entire switch.

### for — The Only Loop

Syntax:
```text
ForStmt    = "for" [ Condition | ForClause | RangeClause ] Block .
Condition  = Expression .
ForClause  = [ InitStmt ] ";" [ Condition ] ";" [ PostStmt ] .
RangeClause = [ identifier_list "=" | identifier_list ":=" ] "range" Expression .
```

```go
// complete for (init; condition; post)
for i := 0; i < 10; i++ {
    sum += i
}

// condition only (while-style)
for sum < 1000 {
    sum += sum
}

// infinite
for {
    break
}

// range over slice/array
for i, v := range slice { }

// range over map (order random)
for k, v := range myMap { }

// range over string (yields runes, not bytes)
for i, r := range "hello" {
    fmt.Printf("%d: %c\n", i, r)   // i = byte offset, r = rune
}

// range over channel (receives until closed)
for v := range ch { }

// range over nothing (skip index and value)
for range ch { }                   // Go 1.22+
```

**Rules:**
- This is the **only** looping construct in Go (no `while`, `do-while`).
- `break` exits the innermost `for`/`switch`/`select`.
- `continue` skips to the next iteration.
- Go 1.22+: loop variables have per-iteration scope (no more loop closure bugs).
- No comma operator.

### defer

Syntax:
```text
DeferStmt = "defer" Expression .
```

```go
defer f.Close()                    // runs when enclosing function returns
defer func() {
    if r := recover(); r != nil {
        log.Println("recovered:", r)
    }
}()
```

**Rules:**
- Expression must be a function or method call.
- Arguments are **evaluated immediately** (at the `defer` site), not deferred.
- Deferred calls execute in **LIFO order** (last deferred = first executed).
- Deferred functions can read and modify **named return values**.
- Commonly used for cleanup: `Close`, `Unlock`, free resources.

---

## 6. Functions

### Function Declaration

Syntax:
```text
FunctionDecl = "func" FunctionName [ TypeParameters ] Signature [ Block ] .
Block        = "{" StatementList "}" .
```

```go
func add(x int, y int) int {
    return x + y
}

func swap(a, b string) (string, string) {
    return b, a
}

func split(sum int) (x, y int) {
    x = sum * 4 / 9
    y = sum - x
    return                       // "naked" return — returns x, y
}

func sum(nums ...int) int {
    total := 0
    for _, n := range nums {
        total += n
    }
    return total
}
```

### Signature Details

- Consecutive parameters of the same type: `(x, y int)`.
- Variadic parameter: `xs ...T` — accepts zero or more `T` arguments; `xs` has type `[]T` inside the function.
- Only the **last** parameter can be variadic.
- Named return values act as variables declared at the top of the function.

### Rules

- A function without a return type cannot contain `return` with values.
- A function with a return type **must** have `return` on all paths (or `panic`/`os.Exit`).
- `func main()` has no parameters and no return type — it is the program entry point.
- `func init()` has no parameters and no return type — it runs automatically per file at program startup, in declaration order.
- Functions are first-class: can be assigned to variables, passed as arguments, returned from other functions.

---

## 7. Methods

Syntax:
```text
MethodDecl = "func" Receiver MethodName Signature [ Block ] .
Receiver   = Parameters .
```

```go
func (v Vertex) Abs() float64          // value receiver
func (v *Vertex) Scale(f float64)       // pointer receiver
```

### Value vs Pointer Receiver

| Aspect | Value Receiver | Pointer Receiver |
|---|---|---|
| Can modify the receiver? | No (operates on copy) | Yes |
| Large structs? | Copies entire struct | Efficient (passes pointer) |
| Nil receiver safety | Depends on implementation | Can check `if v == nil` |
| Automatically called on | value or pointer | value or pointer |
| Consistent usage | — | Convention: if one method uses pointer receiver, all should |

**Rules:**
- Methods can be declared on **any type** defined in the same package (not on built-in types or types from other packages).
- Go automatically handles `&v` for pointer receiver calls on addressable values, and `*p` for value receiver calls on pointers.
- You cannot declare a method with a receiver whose type is defined in another package.

---

## 8. Interfaces

### Implicit Satisfaction

A type `T` implements an interface `I` if `T`'s method set contains all methods of `I`.
No declaration of intent needed — satisfaction is structural.

```go
type Writer interface {
    Write([]byte) (int, error)
}

type MyWriter struct{}
func (w MyWriter) Write(p []byte) (int, error) {
    return len(p), nil
}

var w Writer = MyWriter{}       // ok, MyWriter implements Writer
```

### Interface Embedding

```go
type ReadWriter interface {
    Reader        // all methods of Reader
    Writer        // all methods of Writer
}
```

### Nil Interface vs Non-Nil Concrete Value

```go
var v interface{}          // v == nil (type=nil, value=nil)

var p *int = nil
v = p                      // v != nil (type=*int, value=nil)
```

**Critical rule:** An interface is `nil` only when **both** the type and value are `nil`.
Storing a `nil` pointer in an interface produces a non-nil interface.

### Type Assertion

Syntax:
```text
TypeAssertion = PrimaryExpr "." "(" Type ")" .
```

```go
v := x.(T)                 // panics if x is not T
v, ok := x.(T)             // ok=false if x is not T, no panic
```

**Rules:**
- `x` must have an interface type (not a concrete type).
- If `T` is concrete: checks whether `x` holds a value of type `T`.
- If `T` is an interface: checks whether `x`'s value satisfies `T`.
- The two-value form (`v, ok`) avoids panic.

---

## 9. Generics (Go 1.18+)

### Type Parameters

Syntax:
```text
TypeParameters = "[" TypeParamList "]" .
TypeParamList  = TypeParamDecl { "," TypeParamDecl } .
TypeParamDecl  = identifier_list TypeConstraint .
TypeConstraint = TypeElem .
```

```go
// generic function
func Index[T comparable](s []T, x T) int

// multiple type parameters
func Map[T, U any](s []T, f func(T) U) []U

// generic type
type Stack[T any] struct {
    items []T
}
func (s *Stack[T]) Push(v T) { ... }
```

### Type Inference

Go can often infer type arguments from function call arguments:
```go
s := []int{1, 2, 3}
Index(s, 2)            // T inferred as int (no need for Index[int](s, 2))
```

### Constraint Patterns

```go
any                         // no constraint
comparable                  // supports == and !=
interface { int | string }                  // union
interface { ~int }                          // any type with underlying int
interface { ~int | ~string }                // union with underlying types
interface { int; String() string }          // type + method
```

**Rules:**
- Type parameters are declared in square brackets before the function name or type name.
- Constraints are interfaces (possibly containing type terms).
- `~T` matches types whose **underlying type** is `T` (e.g., `type MyInt int` matches `~int`).
- Type inference only works with function arguments, not with return types alone.
- Generic types must be instantiated: `var s Stack[int]`.

---

## 10. Concurrency

### Goroutines

```go
go f(args...)                   // starts f in a new goroutine
```

- `go` keyword launches a lightweight thread managed by the Go runtime.
- Goroutines multiplex onto OS threads (M:N scheduling).
- The program **exits when `main` returns** — it does not wait for other goroutines.
- All goroutines share the same address space.
- Anonymous function closures capture variables by reference.

### Channels

Syntax:
```go
ch := make(chan int)             // unbuffered (synchronous)
ch := make(chan int, 100)        // buffered (asynchronous)
```

| Operation | Unbuffered | Buffered | `nil` Channel | Closed Channel |
|---|---|---|---|---|
| Send `ch <- v` | blocks until receiver ready | blocks if buffer full | blocks forever | **panic** |
| Receive `<-ch` | blocks until sender ready | blocks if empty | blocks forever | returns zero + `false` |
| `close(ch)` | ok | ok | **panic** | **panic** |
| `range ch` | waits for close | iterates buffered, then waits | blocks forever | empties remaining, exits |

### Directional Channels

```go
func f(out chan<- int, in <-chan int) {
    out <- 42               // ok: send only
    v := <-in               // ok: receive only
    <-out                   // compile error: receive from send-only
    in <- 42                // compile error: send to receive-only
}
```

### Channel Axioms

1. Send to `nil` channel blocks forever.
2. Receive from `nil` channel blocks forever.
3. `close(nil)` panics.
4. Send to closed channel panics.
5. Receive from closed channel returns zero + `false` (ok).
6. Close of already-closed channel panics.
7. Range over `nil` channel blocks forever.
8. Range over closed channel completes immediately.

### select

Syntax:
```text
SelectStmt = "select" "{" { CommClause } "}" .
CommClause = CommCase ":" StatementList .
CommCase   = "case" ( SendStmt | RecvStmt ) | "default" .
RecvStmt   = [ identifier_list ( "=" | ":=" ) ] RecvExpr .
```

```go
select {
case v := <-ch1:
    fmt.Println(v)
case ch2 <- v:
    fmt.Println("sent")
case <-time.After(1 * time.Second):
    fmt.Println("timeout")
default:
    fmt.Println("no comm ready")
}
```

**Rules:**
- Blocks until one `case` can proceed.
- If multiple cases are ready simultaneously, one is chosen **uniformly pseudo-random**.
- `default` is always ready and makes the select **non-blocking**.
- `select {}` blocks forever (no cases, no default).
- A `nil` channel case is never selected.

### sync.Mutex

```go
var mu sync.Mutex

mu.Lock()
// critical section
mu.Unlock()

// idiomatic with defer
mu.Lock()
defer mu.Unlock()
```

### sync.RWMutex

```go
var mu sync.RWMutex

mu.RLock()              // multiple concurrent readers allowed
// read section
mu.RUnlock()

mu.Lock()               // exclusive access for writer
// write section
mu.Unlock()
```

### sync.WaitGroup

```go
var wg sync.WaitGroup

wg.Add(1)               // must be called before goroutine starts
go func() {
    defer wg.Done()     // decrement counter when done
    work()
}()
wg.Wait()               // block until counter reaches 0
```

**Rules:**
- `Add` must be called **before** spawning the goroutine (not inside it).
- `WaitGroup` must not be copied after first use.
- All `Add` calls should happen in the same goroutine as `Wait`.

### sync.Once

```go
var once sync.Once
once.Do(func() {
    // executed exactly once, even across goroutines
    initResource()
})
```

### Context

```go
ctx, cancel := context.WithCancel(context.Background())
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
ctx := context.WithValue(parentCtx, key, val)

select {
case <-ctx.Done():
    return ctx.Err()             // Canceled or DeadlineExceeded
case result := <-ch:
    return result
}
```

---

## 11. Error Handling

### The error Interface

```go
type error interface {
    Error() string
}
```

### Idiomatic Error Handling

Errors are **values** — return them, check them, wrap them:

```go
// every function that can fail returns (result, error)
f, err := os.Open(filename)
if err != nil {
    return fmt.Errorf("open %s: %w", filename, err)
}

// sentinel errors
var ErrNotFound = errors.New("not found")
if errors.Is(err, ErrNotFound) { ... }

// custom error types
type MyError struct {
    Code int
    Msg  string
}
func (e *MyError) Error() string {
    return fmt.Sprintf("code %d: %s", e.Code, e.Msg)
}
var target *MyError
if errors.As(err, &target) { ... }
```
---

## 12. Common Built-in Functions

| Function | Signature | Description |
|---|---|---|
| `len` | `(s) int` | length of string/array/slice/map/channel |
| `cap` | `(s) int` | capacity of slice/array/channel |
| `append` | `(s []T, vs ...T) []T` | append elements to a slice |
| `copy` | `(dst, src []T) int` | copy elements between slices |
| `make` | `(T, size, ...cap) T` | create slice/map/channel |
| `new` | `(T) *T` | allocate zero value, return pointer |
| `delete` | `(m map[K]V, k K)` | delete map entry |
| `close` | `(c chan T)` | close a channel |
| `panic` | `(v any)` | trigger a panic |
| `recover` | `() any` | recover from a panic |
| `clear` | `(m map[K]V)` or `(s []T)` | clear all entries/zero elements (Go 1.21) |
| `min`/`max` | `(x, y ...T) T` | min/max of ordered values (Go 1.21) |

---

## 13. Structural Rules Summary

| Rule | Description |
|---|---|
| Unused local variable | Compile error (except `_`) |
| Unused import | Compile error |
| `{` placement | Must be on same line as the keyword (`if`, `for`, `switch`, `func`) |
| Semicolons | Injected automatically by the lexer — not written in source |
| Exported names | Start with uppercase letter |
| Type after name | `v Type` syntax (not `Type v` like C/Java) |
| Visibility | By identifier case, not by keyword |
| `=` vs `:=` | `:=` declares new variable(s); `=` assigns to existing |
| No ternary `?:` | Use if-else |
| No `while`/`do` | Use `for` with condition |
| No classes | Types + methods instead |
| No inheritance | Composition + interfaces |
| No exceptions | Errors are values; panic is only for truly exceptional situations |
| No implicit conversions | All numeric conversions must be explicit |
| nil slice vs nil map | `append`/`len`/`cap` on nil slice is safe; writing to nil map panics |
