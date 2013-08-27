// Package pq is a pure Go Postgres driver for the database/sql package.
package pq

import (
	"bufio"
	"crypto/md5"
	"crypto/tls"
	"database/sql"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/lib/pq/oid"
	"io"
	"net"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
)

var (
	ErrSSLNotSupported = errors.New("pq: SSL is not enabled on the server")
	ErrNotSupported    = errors.New("pq: invalid command")
)

type drv struct{}

func (d *drv) Open(name string) (driver.Conn, error) {
	return Open(name)
}

func init() {
	sql.Register("postgres", &drv{})
	connPool = make(map[string] *connPoolItem)
}

type conn struct {
	c       net.Conn
	buf     *bufio.Reader
	namei   int
	scratch [512]byte
	pool  *connPoolItem
	err   error
}

func (c *conn) writeBuf(b byte) *writeBuf {
	c.scratch[0] = b
	w := writeBuf(c.scratch[:5])
	return &w
}

type connPoolItem struct {
	conns int
	free  []*conn
	mu sync.Mutex
	sem   chan int

	maxcons int
	persist bool // if not persistent, then connection is droped on Close()
}

// The pool of all databases; one pool item per unique database connection string.
var connPool map[string] *connPoolItem
var connPoolMtx sync.Mutex

func Open(name string) (_ driver.Conn, err error) {
	var pi *connPoolItem

	defer func() {
		if err != nil && pi != nil {
			<-pi.sem
		}
	}()

	defer errRecover(&err)
	defer errRecoverWithPGReason(&err)

	o := make(Values)

	// A number of defaults are applied here, in this order:
	//
	// * Very low precedence defaults applied in every situation
	// * Environment variables
	// * Explicitly passed connection information
	o.Set("host", "localhost")
	o.Set("port", "5432")

	for k, v := range parseEnviron(os.Environ()) {
		o.Set(k, v)
	}

	parseOpts(name, o)

	smaxconns := o.Get("maxcons")
	maxcn := int(0)
	if smaxconns != "" {
		if maxcn, err = strconv.Atoi(smaxconns); err != nil {
			return nil, errors.New("Invalid maxcons")
		}
	}

	spersist := o.Get("persist")
	var persist bool = spersist == "true" || spersist == "yes"

	// Unique connection name for pool
	connName := o.Get("host") + ":" + o.Get("port") + "/" + o.Get("dbname")

	if maxcn > 0 {
		connPoolMtx.Lock()
		pi, _ = connPool[connName]
		if pi == nil {
			pi = &connPoolItem {
				conns: 0,
				free: make([]*conn, 0),
				sem: make(chan int, maxcn),
				maxcons: maxcn,
				persist: persist,
			}

			pi.sem <- 1

			connPool[connName] = pi

			connPoolMtx.Unlock()
		} else {
			connPoolMtx.Unlock()
			pi.sem <- 1
			if pi.persist {
				pi.mu.Lock()
				if len(pi.free) > 0 {
					conn := pi.free[len(pi.free)-1]
					pi.free = pi.free[:len(pi.free)-1]
					pi.mu.Unlock()
					return conn, nil
				} else if pi.conns > pi.maxcons {
					// Semaphore should've limited number of connections to maxcons
					panic("Semaphore error. Maximum connections reached.")
				}
				pi.mu.Unlock()
			}
		}
	}

	// If a user is not provided by any other means, the last
	// resort is to use the current operating system provided user
	// name.
	if o.Get("user") == "" {
		u, err := userCurrent()
		if err != nil {
			return nil, err
		} else {
			o.Set("user", u)
		}
	}

	c, err := net.Dial(network(o))
	if err != nil {
		return nil, err
	}

	cn := &conn{c: c}
	cn.ssl(o)
	cn.buf = bufio.NewReader(cn.c)
	cn.startup(o)

	if maxcn > 0 {
		cn.pool = pi
		pi.mu.Lock()
		pi.conns++
		pi.mu.Unlock()
	}
	return cn, nil
}

func network(o Values) (string, string) {
	host := o.Get("host")

	if strings.HasPrefix(host, "/") {
		sockPath := path.Join(host, ".s.PGSQL."+o.Get("port"))
		return "unix", sockPath
	}

	return "tcp", host + ":" + o.Get("port")
}

type Values map[string]string

func (vs Values) Set(k, v string) {
	vs[k] = v
}

func (vs Values) Get(k string) (v string) {
	return vs[k]
}

func parseOpts(name string, o Values) {
	if len(name) == 0 {
		return
	}

	name = strings.TrimSpace(name)

	ps := strings.Split(name, " ")
	for _, p := range ps {
		kv := strings.Split(p, "=")
		if len(kv) < 2 {
			errorf("invalid option: %q", p)
		}
		o.Set(kv[0], kv[1])
	}
}

func (cn *conn) Begin() (driver.Tx, error) {
	_, err := cn.Exec("BEGIN", nil)
	if err != nil {
		return nil, err
	}
	return cn, err
}

func (cn *conn) Commit() error {
	_, err := cn.Exec("COMMIT", nil)
	return err
}

func (cn *conn) Rollback() error {
	_, err := cn.Exec("ROLLBACK", nil)
	return err
}

func (cn *conn) gname() string {
	cn.namei++
	return strconv.FormatInt(int64(cn.namei), 10)
}

func (cn *conn) simpleQuery(q string) (res driver.Result, err error) {
	defer func() { cn.err = err }()
	defer errRecover(&err)

	b := cn.writeBuf('Q')
	b.string(q)
	cn.send(b)

	for {
		t, r := cn.recv1()
		switch t {
		case 'C':
			res = parseComplete(r.string())
		case 'Z':
			// done
			return
		case 'E':
			err = parseError(r)
		case 'T', 'N', 'S', 'D':
			// ignore
		default:
			errorf("unknown response for simple query: %q", t)
		}
	}
	panic("not reached")
}

func (cn *conn) prepareTo(q, stmtName string) (_ driver.Stmt, err error) {
	defer func() { cn.err = err }()
	defer errRecover(&err)

	st := &stmt{cn: cn, name: stmtName, query: q}

	b := cn.writeBuf('P')
	b.string(st.name)
	b.string(q)
	b.int16(0)
	cn.send(b)

	b = cn.writeBuf('D')
	b.byte('S')
	b.string(st.name)
	cn.send(b)

	cn.send(cn.writeBuf('S'))

	for {
		t, r := cn.recv1()
		switch t {
		case '1', '2', 'N':
		case 't':
			nparams := int(r.int16())
			st.paramTyps = make([]oid.Oid, nparams)

			for i := range st.paramTyps {
				st.paramTyps[i] = r.oid()
			}
		case 'T':
			n := r.int16()
			st.cols = make([]string, n)
			st.rowTyps = make([]oid.Oid, n)
			for i := range st.cols {
				st.cols[i] = r.string()
				r.next(6)
				st.rowTyps[i] = r.oid()
				r.next(8)
			}
		case 'n':
			// no data
		case 'Z':
			return st, err
		case 'E':
			err = parseError(r)
		case 'C':
			// command complete
			return st, err
		default:
			errorf("unexpected describe rows response: %q", t)
		}
	}

	panic("not reached")
}

func (cn *conn) Prepare(q string) (driver.Stmt, error) {
	return cn.prepareTo(q, cn.gname())
}

func (cn *conn) Close() (err error) {
	defer func() { cn.err = err }()
	defer errRecover(&err)

	// For persistent connection, do not close it; put it back into the free list
	// An error condition on the connection will Close it outright.
	if cn.pool != nil && cn.pool.maxcons > 0 {
		if cn.err == nil && cn.pool.persist {
			cn.pool.mu.Lock()
			cn.pool.free = append(cn.pool.free, cn)
			cn.pool.mu.Unlock()

			<-cn.pool.sem
			return nil
		}

		cn.pool.mu.Lock()
		cn.pool.conns--
		cn.pool.mu.Unlock()
		<-cn.pool.sem
	}

	cn.send(cn.writeBuf('X'))
	err = cn.c.Close()

	return err
}

// Implement the optional "Execer" interface for one-shot queries
func (cn *conn) Exec(query string, args []driver.Value) (_ driver.Result, err error) {
	defer func() { cn.err = err }()
	defer errRecover(&err)

	// Check to see if we can use the "simpleQuery" interface, which is
	// *much* faster than going through prepare/exec
	if len(args) == 0 {
		return cn.simpleQuery(query)
	}

	// Use the unnamed statement to defer planning until bind
	// time, or else value-based selectivity estimates cannot be
	// used.
	st, err := cn.prepareTo(query, "")
	if err != nil {
		panic(err)
	}

	r, err := st.Exec(args)
	if err != nil {
		panic(err)
	}

	return r, err
}

// Assumes len(*m) is > 5
func (cn *conn) send(m *writeBuf) {
	b := (*m)[1:]
	binary.BigEndian.PutUint32(b, uint32(len(b)))

	if (*m)[0] == 0 {
		*m = b
	}

	_, err := cn.c.Write(*m)
	if err != nil {
		panic(err)
	}
}

func (cn *conn) recv() (t byte, r *readBuf) {
	for {
		t, r = cn.recv1()
		switch t {
		case 'E':
			panic(parseError(r))
		case 'N':
			// ignore
		default:
			return
		}
	}

	panic("not reached")
}

func (cn *conn) recv1() (byte, *readBuf) {
	x := cn.scratch[:5]
	_, err := io.ReadFull(cn.buf, x)
	if err != nil {
		panic(err)
	}
	c := x[0]

	b := readBuf(x[1:])
	n := b.int32() - 4
	var y []byte
	if n <= len(cn.scratch) {
		y = cn.scratch[:n]
	} else {
		y = make([]byte, n)
	}
	_, err = io.ReadFull(cn.buf, y)
	if err != nil {
		panic(err)
	}

	return c, (*readBuf)(&y)
}

func (cn *conn) ssl(o Values) {
	tlsConf := tls.Config{}
	switch mode := o.Get("sslmode"); mode {
	case "require", "":
		tlsConf.InsecureSkipVerify = true
	case "verify-full":
		// fall out
	case "disable":
		return
	default:
		errorf(`unsupported sslmode %q; only "require" (default), "verify-full", and "disable" supported`, mode)
	}

	w := cn.writeBuf(0)
	w.int32(80877103)
	cn.send(w)

	b := cn.scratch[:1]
	_, err := io.ReadFull(cn.c, b)
	if err != nil {
		panic(err)
	}

	if b[0] != 'S' {
		panic(ErrSSLNotSupported)
	}

	cn.c = tls.Client(cn.c, &tlsConf)
}

func (cn *conn) startup(o Values) {
	w := cn.writeBuf(0)
	w.int32(196608)
	w.string("user")
	w.string(o.Get("user"))
	w.string("database")
	w.string(o.Get("dbname"))
	w.string("")
	cn.send(w)

	for {
		t, r := cn.recv()
		switch t {
		case 'K', 'S':
		case 'R':
			cn.auth(r, o)
		case 'Z':
			return
		default:
			errorf("unknown response for startup: %q", t)
		}
	}
}

func (cn *conn) auth(r *readBuf, o Values) {
	switch code := r.int32(); code {
	case 0:
		// OK
	case 3:
		w := cn.writeBuf('p')
		w.string(o.Get("password"))
		cn.send(w)

		t, r := cn.recv()
		if t != 'R' {
			errorf("unexpected password response: %q", t)
		}

		if r.int32() != 0 {
			errorf("unexpected authentication response: %q", t)
		}
	case 5:
		s := string(r.next(4))
		w := cn.writeBuf('p')
		w.string("md5" + md5s(md5s(o.Get("password")+o.Get("user"))+s))
		cn.send(w)

		t, r := cn.recv()
		if t != 'R' {
			errorf("unexpected password response: %q", t)
		}

		if r.int32() != 0 {
			errorf("unexpected authentication response: %q", t)
		}
	default:
		errorf("unknown authentication response: %d", code)
	}
}

type stmt struct {
	cn        *conn
	name      string
	query     string
	cols      []string
	rowTyps   []oid.Oid
	paramTyps []oid.Oid
	closed    bool
}

func (st *stmt) Close() (err error) {
	if st.closed {
		return nil
	}

	defer func() { st.cn.err = err }()
	defer errRecover(&err)

	w := st.cn.writeBuf('C')
	w.byte('S')
	w.string(st.name)
	st.cn.send(w)

	st.cn.send(st.cn.writeBuf('S'))

	t, _ := st.cn.recv()
	if t != '3' {
		errorf("unexpected close response: %q", t)
	}
	st.closed = true

	t, _ = st.cn.recv()
	if t != 'Z' {
		errorf("expected ready for query, but got: %q", t)
	}

	return nil
}

func (st *stmt) Query(v []driver.Value) (_ driver.Rows, err error) {
	defer func() { st.cn.err = err }()
	defer errRecover(&err)
	st.exec(v)
	return &rows{st: st}, nil
}

func (st *stmt) Exec(v []driver.Value) (res driver.Result, err error) {
	defer func() { st.cn.err = err }()
	defer errRecover(&err)

	if len(v) == 0 {
		return st.cn.simpleQuery(st.query)
	}
	st.exec(v)

	for {
		t, r := st.cn.recv1()
		switch t {
		case 'E':
			err = parseError(r)
		case 'C':
			res = parseComplete(r.string())
		case 'Z':
			// done
			return
		case 'T', 'N', 'S', 'D':
			// Ignore
		default:
			errorf("unknown exec response: %q", t)
		}
	}

	panic("not reached")
}

func (st *stmt) exec(v []driver.Value) {
	w := st.cn.writeBuf('B')
	w.string("")
	w.string(st.name)
	w.int16(0)
	w.int16(len(v))
	for i, x := range v {
		if x == nil {
			w.int32(-1)
		} else {
			b := encode(x, st.paramTyps[i])
			w.int32(len(b))
			w.bytes(b)
		}
	}
	w.int16(0)
	st.cn.send(w)

	w = st.cn.writeBuf('E')
	w.string("")
	w.int32(0)
	st.cn.send(w)

	st.cn.send(st.cn.writeBuf('S'))

	var err error
	for {
		t, r := st.cn.recv1()
		switch t {
		case 'E':
			err = parseError(r)
		case '2':
			if err != nil {
				panic(err)
			}
			return
		case 'Z':
			if err != nil {
				panic(err)
			}
			return
		case 'N':
			// ignore
		default:
			errorf("unexpected bind response: %q", t)
		}
	}
}

func (st *stmt) NumInput() int {
	return len(st.paramTyps)
}

func parseComplete(s string) driver.Result {
	parts := strings.Split(s, " ")
	n, _ := strconv.ParseInt(parts[len(parts)-1], 10, 64)
	return driver.RowsAffected(n)
}

type rows struct {
	st   *stmt
	done bool
}

func (rs *rows) Close() error {
	for {
		err := rs.Next(nil)
		switch err {
		case nil:
		case io.EOF:
			return nil
		default:
			return err
		}
	}
	panic("not reached")
}

func (rs *rows) Columns() []string {
	return rs.st.cols
}

func (rs *rows) Next(dest []driver.Value) (err error) {
	if rs.done {
		return io.EOF
	}

	defer func() { rs.st.cn.err = err }()
	defer errRecover(&err)

	for {
		t, r := rs.st.cn.recv1()
		switch t {
		case 'E':
			err = parseError(r)
		case 'C', 'S', 'N':
			continue
		case 'Z':
			rs.done = true
			if err != nil {
				return err
			}
			return io.EOF
		case 'D':
			n := r.int16()
			if n < len(dest) {
				dest = dest[:n]
			}
			for i := range dest {
				l := r.int32()
				if l == -1 {
					dest[i] = nil
					continue
				}
				dest[i] = decode(r.next(l), rs.st.rowTyps[i])
			}
			return
		default:
			errorf("unexpected message after execute: %q", t)
		}
	}

	panic("not reached")
}

func md5s(s string) string {
	h := md5.New()
	h.Write([]byte(s))
	return fmt.Sprintf("%x", h.Sum(nil))
}

// parseEnviron tries to mimic some of libpq's environment handling
//
// To ease testing, it does not directly reference os.Environ, but is
// designed to accept its output.
//
// Environment-set connection information is intended to have a higher
// precedence than a library default but lower than any explicitly
// passed information (such as in the URL or connection string).
func parseEnviron(env []string) (out map[string]string) {
	out = make(map[string]string)

	for _, v := range env {
		parts := strings.SplitN(v, "=", 2)

		accrue := func(keyname string) {
			out[keyname] = parts[1]
		}

		// The order of these is the same as is seen in the
		// PostgreSQL 9.1 manual, with omissions briefly
		// noted.
		switch parts[0] {
		case "PGHOST":
			accrue("host")
		case "PGHOSTADDR":
			accrue("hostaddr")
		case "PGPORT":
			accrue("port")
		case "PGDATABASE":
			accrue("dbname")
		case "PGUSER":
			accrue("user")
		case "PGPASSWORD":
			accrue("password")
		// skip PGPASSFILE, PGSERVICE, PGSERVICEFILE,
		// PGREALM
		case "PGOPTIONS":
			accrue("options")
		case "PGAPPNAME":
			accrue("application_name")
		case "PGSSLMODE":
			accrue("sslmode")
		case "PGREQUIRESSL":
			accrue("requiressl")
		case "PGSSLCERT":
			accrue("sslcert")
		case "PGSSLKEY":
			accrue("sslkey")
		case "PGSSLROOTCERT":
			accrue("sslrootcert")
		case "PGSSLCRL":
			accrue("sslcrl")
		case "PGREQUIREPEER":
			accrue("requirepeer")
		case "PGKRBSRVNAME":
			accrue("krbsrvname")
		case "PGGSSLIB":
			accrue("gsslib")
		case "PGCONNECT_TIMEOUT":
			accrue("connect_timeout")
		case "PGCLIENTENCODING":
			accrue("client_encoding")
			// skip PGDATESTYLE, PGTZ, PGGEQO, PGSYSCONFDIR,
			// PGLOCALEDIR
		case "PGMAXCONS":
			accrue("maxcons")
		case "PGPERSIST":
			accrue("persist")
		}
	}

	return out
}
