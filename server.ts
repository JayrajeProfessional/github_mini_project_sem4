import express, {
  type Request,
  type Response,
  type NextFunction,
  type RequestHandler,
} from "express";
import { Pool } from "pg";
import cors from "cors";

// ─────────────────────────────────────────────────────────────────────────────
// ENV VALIDATION
// ─────────────────────────────────────────────────────────────────────────────

const PORT = process.env.PORT ?? "5000";
const DATABASE_URL = process.env.DATABASE_URL;
const CORS_ORIGIN = process.env.CORS_ORIGIN ?? "*";
const NODE_ENV = process.env.NODE_ENV ?? "development";

if (!DATABASE_URL) {
  console.error("FATAL: DATABASE_URL environment variable is not set.");
  process.exit(1);
}

// ─────────────────────────────────────────────────────────────────────────────
// DATABASE POOL  (Supabase Postgres via standard pg driver — no Supabase SDK)
// ─────────────────────────────────────────────────────────────────────────────

const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: { rejectUnauthorized: false }, // required for Supabase / Railway TLS
  max: 10,
  idleTimeoutMillis: 30_000,
  connectionTimeoutMillis: 5_000,
});

pool.on("error", (err) => {
  console.error("Unexpected pg pool error:", err.message);
});

// ─────────────────────────────────────────────────────────────────────────────
// DATABASE INITIALISATION  (run once on startup)
// ─────────────────────────────────────────────────────────────────────────────

async function initDB(): Promise<void> {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    await client.query(`
      CREATE TABLE IF NOT EXISTS books (
        id          SERIAL PRIMARY KEY,
        title       VARCHAR(255) NOT NULL,
        author      VARCHAR(255) NOT NULL,
        isbn        VARCHAR(20)  UNIQUE,
        available   BOOLEAN      NOT NULL DEFAULT TRUE,
        created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
        updated_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
      );
    `);

    await client.query(`
      CREATE TABLE IF NOT EXISTS members (
        id          SERIAL PRIMARY KEY,
        name        VARCHAR(255) NOT NULL,
        email       VARCHAR(255) NOT NULL UNIQUE,
        phone       VARCHAR(20),
        created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
        updated_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
      );
    `);

    await client.query(`
      CREATE TABLE IF NOT EXISTS transactions (
        id          SERIAL PRIMARY KEY,
        book_id     INT          NOT NULL REFERENCES books(id)   ON DELETE RESTRICT ON UPDATE CASCADE,
        member_id   INT          NOT NULL REFERENCES members(id) ON DELETE RESTRICT ON UPDATE CASCADE,
        issue_date  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
        return_date TIMESTAMPTZ,
        created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
        updated_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
      );
    `);

    // Indexes
    await client.query(`CREATE INDEX IF NOT EXISTS idx_transactions_book_id   ON transactions(book_id);`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_transactions_member_id ON transactions(member_id);`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_books_title            ON books(title);`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_books_author           ON books(author);`);

    // Auto-update updated_at via trigger
    await client.query(`
      CREATE OR REPLACE FUNCTION set_updated_at()
      RETURNS TRIGGER LANGUAGE plpgsql AS $$
      BEGIN NEW.updated_at = NOW(); RETURN NEW; END; $$;
    `);

    for (const tbl of ["books", "members", "transactions"]) {
      await client.query(`
        DO $$ BEGIN
          IF NOT EXISTS (
            SELECT 1 FROM pg_trigger
            WHERE tgname = 'trg_${tbl}_updated_at'
          ) THEN
            CREATE TRIGGER trg_${tbl}_updated_at
            BEFORE UPDATE ON ${tbl}
            FOR EACH ROW EXECUTE FUNCTION set_updated_at();
          END IF;
        END $$;
      `);
    }

    await client.query("COMMIT");
    console.log("✅  Database initialised successfully.");
  } catch (err) {
    await client.query("ROLLBACK");
    console.error("❌  Database init failed:", err);
    throw err;
  } finally {
    client.release();
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// TYPES
// ─────────────────────────────────────────────────────────────────────────────

interface Book {
  id: number;
  title: string;
  author: string;
  isbn: string | null;
  available: boolean;
  created_at: Date;
  updated_at: Date;
}

interface Member {
  id: number;
  name: string;
  email: string;
  phone: string | null;
  created_at: Date;
  updated_at: Date;
}

interface Transaction {
  id: number;
  book_id: number;
  member_id: number;
  issue_date: Date;
  return_date: Date | null;
  created_at: Date;
  updated_at: Date;
}

// ─────────────────────────────────────────────────────────────────────────────
// RESPONSE HELPERS
// ─────────────────────────────────────────────────────────────────────────────

function ok<T>(res: Response, data: T, message: string, status = 200): void {
  res.status(status).json({ success: true, message, data });
}

function fail(
  res: Response,
  message: string,
  status: number,
  errors?: { field: string; message: string }[]
): void {
  res.status(status).json({ success: false, message, ...(errors ? { errors } : {}) });
}

// ─────────────────────────────────────────────────────────────────────────────
// VALIDATION HELPERS
// ─────────────────────────────────────────────────────────────────────────────

type ValidationError = { field: string; message: string };

function validateString(
  value: unknown,
  field: string,
  required: boolean,
  maxLen = 255
): ValidationError | null {
  if (required && (value === undefined || value === null || value === "")) {
    return { field, message: `${field} is required` };
  }
  if (value !== undefined && value !== null && value !== "") {
    if (typeof value !== "string") return { field, message: `${field} must be a string` };
    if (value.length > maxLen)
      return { field, message: `${field} must be at most ${maxLen} characters` };
  }
  return null;
}

function validateEmail(value: unknown, field: string): ValidationError | null {
  const strErr = validateString(value, field, true);
  if (strErr) return strErr;
  const emailRe = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
  if (!emailRe.test(value as string)) return { field, message: "Email must be a valid email address" };
  return null;
}

function validateInt(value: unknown, field: string, required: boolean): ValidationError | null {
  if (required && (value === undefined || value === null)) {
    return { field, message: `${field} is required` };
  }
  if (value !== undefined && value !== null) {
    const n = Number(value);
    if (!Number.isInteger(n) || n <= 0) {
      return { field, message: `${field} must be a positive integer` };
    }
  }
  return null;
}

// ─────────────────────────────────────────────────────────────────────────────
// ASYNC HANDLER WRAPPER
// ─────────────────────────────────────────────────────────────────────────────

function asyncHandler(
  fn: (req: Request, res: Response, next: NextFunction) => Promise<void>
): RequestHandler {
  return (req, res, next) => fn(req, res, next).catch(next);
}

// ─────────────────────────────────────────────────────────────────────────────
// EXPRESS APP
// ─────────────────────────────────────────────────────────────────────────────

const app = express();

app.use(
  cors({
    origin: CORS_ORIGIN === "*" ? "*" : CORS_ORIGIN.split(",").map((s) => s.trim()),
    methods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allowedHeaders: ["Content-Type", "Accept"],
  })
);
app.use(express.json());

// ─────────────────────────────────────────────────────────────────────────────
// HEALTH
// ─────────────────────────────────────────────────────────────────────────────

app.get("/health", asyncHandler(async (_req, res) => {
  await pool.query("SELECT 1");
  ok(res.status(200), null, "Service is healthy");
}));

// ─────────────────────────────────────────────────────────────────────────────
// BOOKS ROUTES
// ─────────────────────────────────────────────────────────────────────────────

// POST /api/books
app.post(
  "/api/books",
  asyncHandler(async (req, res) => {
    const { title, author, isbn } = req.body ?? {};
    const errors: ValidationError[] = [];

    const e1 = validateString(title, "title", true);
    const e2 = validateString(author, "author", true);
    const e3 = isbn !== undefined ? validateString(isbn, "isbn", false, 20) : null;
    if (e1) errors.push(e1);
    if (e2) errors.push(e2);
    if (e3) errors.push(e3);
    if (errors.length) return fail(res, "Validation failed", 400, errors);

    try {
      const { rows } = await pool.query<Book>(
        `INSERT INTO books (title, author, isbn)
         VALUES ($1, $2, $3)
         RETURNING *`,
        [title as string, author as string, isbn ?? null]
      );
      ok(res, rows[0], "Book created successfully", 201);
    } catch (err: unknown) {
      if (isUniqueViolation(err)) return fail(res, "A book with this ISBN already exists", 409);
      throw err;
    }
  })
);

// GET /api/books
app.get(
  "/api/books",
  asyncHandler(async (req, res) => {
    const { available } = req.query;
    let query = "SELECT * FROM books";
    const params: unknown[] = [];

    if (available !== undefined) {
      const val = available === "true" ? true : available === "false" ? false : undefined;
      if (val !== undefined) {
        query += " WHERE available = $1";
        params.push(val);
      }
    }
    query += " ORDER BY created_at DESC";

    const { rows } = await pool.query<Book>(query, params);
    ok(res, rows, "Books fetched successfully");
  })
);

// GET /api/books/:id
app.get(
  "/api/books/:id",
  asyncHandler(async (req, res) => {
    const id = Number(req.params.id);
    if (!Number.isInteger(id) || id <= 0) return fail(res, "Invalid book ID", 400);

    const { rows } = await pool.query<Book>("SELECT * FROM books WHERE id = $1", [id]);
    if (!rows[0]) return fail(res, "Book not found", 404);
    ok(res, rows[0], "Book fetched successfully");
  })
);

// PUT /api/books/:id
app.put(
  "/api/books/:id",
  asyncHandler(async (req, res) => {
    const id = Number(req.params.id);
    if (!Number.isInteger(id) || id <= 0) return fail(res, "Invalid book ID", 400);

    const { title, author, isbn } = req.body ?? {};
    const errors: ValidationError[] = [];

    const e1 = title !== undefined ? validateString(title, "title", true) : null;
    const e2 = author !== undefined ? validateString(author, "author", true) : null;
    const e3 = isbn !== undefined ? validateString(isbn, "isbn", false, 20) : null;
    if (e1) errors.push(e1);
    if (e2) errors.push(e2);
    if (e3) errors.push(e3);
    if (errors.length) return fail(res, "Validation failed", 400, errors);

    const existing = await pool.query<Book>("SELECT * FROM books WHERE id = $1", [id]);
    if (!existing.rows[0]) return fail(res, "Book not found", 404);

    const fields: string[] = [];
    const vals: unknown[] = [];
    if (title !== undefined) { fields.push(`title = $${fields.length + 1}`); vals.push(title); }
    if (author !== undefined) { fields.push(`author = $${fields.length + 1}`); vals.push(author); }
    if (isbn !== undefined) { fields.push(`isbn = $${fields.length + 1}`); vals.push(isbn); }

    if (!fields.length) return ok(res, existing.rows[0], "Book updated successfully");

    vals.push(id);
    try {
      const { rows } = await pool.query<Book>(
        `UPDATE books SET ${fields.join(", ")} WHERE id = $${vals.length} RETURNING *`,
        vals
      );
      ok(res, rows[0], "Book updated successfully");
    } catch (err) {
      if (isUniqueViolation(err)) return fail(res, "A book with this ISBN already exists", 409);
      throw err;
    }
  })
);

// DELETE /api/books/:id
app.delete(
  "/api/books/:id",
  asyncHandler(async (req, res) => {
    const id = Number(req.params.id);
    if (!Number.isInteger(id) || id <= 0) return fail(res, "Invalid book ID", 400);

    const existing = await pool.query<Book>("SELECT * FROM books WHERE id = $1", [id]);
    if (!existing.rows[0]) return fail(res, "Book not found", 404);

    // Block delete if there is any active (unreturned) transaction
    const active = await pool.query(
      "SELECT id FROM transactions WHERE book_id = $1 AND return_date IS NULL LIMIT 1",
      [id]
    );
    if (active.rows.length)
      return fail(res, "Cannot delete a book that is currently issued", 409);

    await pool.query("DELETE FROM books WHERE id = $1", [id]);
    ok(res, null, "Book deleted successfully");
  })
);

// ─────────────────────────────────────────────────────────────────────────────
// MEMBERS ROUTES
// ─────────────────────────────────────────────────────────────────────────────

// POST /api/members
app.post(
  "/api/members",
  asyncHandler(async (req, res) => {
    const { name, email, phone } = req.body ?? {};
    const errors: ValidationError[] = [];

    const e1 = validateString(name, "name", true);
    const e2 = validateEmail(email, "email");
    const e3 = phone !== undefined ? validateString(phone, "phone", false, 20) : null;
    if (e1) errors.push(e1);
    if (e2) errors.push(e2);
    if (e3) errors.push(e3);
    if (errors.length) return fail(res, "Validation failed", 400, errors);

    try {
      const { rows } = await pool.query<Member>(
        `INSERT INTO members (name, email, phone) VALUES ($1, $2, $3) RETURNING *`,
        [name as string, (email as string).toLowerCase(), phone ?? null]
      );
      ok(res, rows[0], "Member registered successfully", 201);
    } catch (err) {
      if (isUniqueViolation(err)) return fail(res, "A member with this email already exists", 409);
      throw err;
    }
  })
);

// GET /api/members
app.get(
  "/api/members",
  asyncHandler(async (_req, res) => {
    const { rows } = await pool.query<Member>("SELECT * FROM members ORDER BY created_at DESC");
    ok(res, rows, "Members fetched successfully");
  })
);

// GET /api/members/:id
app.get(
  "/api/members/:id",
  asyncHandler(async (req, res) => {
    const id = Number(req.params.id);
    if (!Number.isInteger(id) || id <= 0) return fail(res, "Invalid member ID", 400);

    const { rows } = await pool.query<Member>("SELECT * FROM members WHERE id = $1", [id]);
    if (!rows[0]) return fail(res, "Member not found", 404);
    ok(res, rows[0], "Member fetched successfully");
  })
);

// PUT /api/members/:id
app.put(
  "/api/members/:id",
  asyncHandler(async (req, res) => {
    const id = Number(req.params.id);
    if (!Number.isInteger(id) || id <= 0) return fail(res, "Invalid member ID", 400);

    const { name, email, phone } = req.body ?? {};
    const errors: ValidationError[] = [];

    const e1 = name !== undefined ? validateString(name, "name", true) : null;
    const e2 = email !== undefined ? validateEmail(email, "email") : null;
    const e3 = phone !== undefined ? validateString(phone, "phone", false, 20) : null;
    if (e1) errors.push(e1);
    if (e2) errors.push(e2);
    if (e3) errors.push(e3);
    if (errors.length) return fail(res, "Validation failed", 400, errors);

    const existing = await pool.query<Member>("SELECT * FROM members WHERE id = $1", [id]);
    if (!existing.rows[0]) return fail(res, "Member not found", 404);

    const fields: string[] = [];
    const vals: unknown[] = [];
    if (name !== undefined)  { fields.push(`name  = $${fields.length + 1}`); vals.push(name); }
    if (email !== undefined) { fields.push(`email = $${fields.length + 1}`); vals.push((email as string).toLowerCase()); }
    if (phone !== undefined) { fields.push(`phone = $${fields.length + 1}`); vals.push(phone); }

    if (!fields.length) return ok(res, existing.rows[0], "Member updated successfully");

    vals.push(id);
    try {
      const { rows } = await pool.query<Member>(
        `UPDATE members SET ${fields.join(", ")} WHERE id = $${vals.length} RETURNING *`,
        vals
      );
      ok(res, rows[0], "Member updated successfully");
    } catch (err) {
      if (isUniqueViolation(err)) return fail(res, "A member with this email already exists", 409);
      throw err;
    }
  })
);

// DELETE /api/members/:id
app.delete(
  "/api/members/:id",
  asyncHandler(async (req, res) => {
    const id = Number(req.params.id);
    if (!Number.isInteger(id) || id <= 0) return fail(res, "Invalid member ID", 400);

    const existing = await pool.query<Member>("SELECT * FROM members WHERE id = $1", [id]);
    if (!existing.rows[0]) return fail(res, "Member not found", 404);

    const active = await pool.query(
      "SELECT id FROM transactions WHERE member_id = $1 AND return_date IS NULL LIMIT 1",
      [id]
    );
    if (active.rows.length)
      return fail(res, "Cannot delete a member who has books currently issued", 409);

    await pool.query("DELETE FROM members WHERE id = $1", [id]);
    ok(res, null, "Member deleted successfully");
  })
);

// ─────────────────────────────────────────────────────────────────────────────
// TRANSACTIONS ROUTES
// ─────────────────────────────────────────────────────────────────────────────

// POST /api/transactions/issue
app.post(
  "/api/transactions/issue",
  asyncHandler(async (req, res) => {
    const { bookId, memberId } = req.body ?? {};
    const errors: ValidationError[] = [];

    const e1 = validateInt(bookId, "bookId", true);
    const e2 = validateInt(memberId, "memberId", true);
    if (e1) errors.push(e1);
    if (e2) errors.push(e2);
    if (errors.length) return fail(res, "Validation failed", 400, errors);

    const client = await pool.connect();
    try {
      await client.query("BEGIN");

      const bookRes = await client.query<Book>("SELECT * FROM books WHERE id = $1 FOR UPDATE", [Number(bookId)]);
      if (!bookRes.rows[0]) { await client.query("ROLLBACK"); return fail(res, "Book not found", 404); }
      if (!bookRes.rows[0].available) { await client.query("ROLLBACK"); return fail(res, "Book is already issued and not available", 409); }

      const memberRes = await client.query<Member>("SELECT id FROM members WHERE id = $1", [Number(memberId)]);
      if (!memberRes.rows[0]) { await client.query("ROLLBACK"); return fail(res, "Member not found", 404); }

      const { rows } = await client.query<Transaction>(
        `INSERT INTO transactions (book_id, member_id, issue_date)
         VALUES ($1, $2, NOW()) RETURNING *`,
        [Number(bookId), Number(memberId)]
      );

      await client.query("UPDATE books SET available = FALSE WHERE id = $1", [Number(bookId)]);
      await client.query("COMMIT");

      ok(res, rows[0], "Book issued successfully", 201);
    } catch (err) {
      await client.query("ROLLBACK");
      throw err;
    } finally {
      client.release();
    }
  })
);

// POST /api/transactions/return
app.post(
  "/api/transactions/return",
  asyncHandler(async (req, res) => {
    const { bookId } = req.body ?? {};
    const e = validateInt(bookId, "bookId", true);
    if (e) return fail(res, "Validation failed", 400, [e]);

    const client = await pool.connect();
    try {
      await client.query("BEGIN");

      const txRes = await client.query<Transaction>(
        `SELECT * FROM transactions
         WHERE book_id = $1 AND return_date IS NULL
         ORDER BY issue_date DESC LIMIT 1
         FOR UPDATE`,
        [Number(bookId)]
      );
      if (!txRes.rows[0]) {
        await client.query("ROLLBACK");
        return fail(res, "No active issue found for this book", 404);
      }

      const { rows } = await client.query<Transaction>(
        `UPDATE transactions SET return_date = NOW()
         WHERE id = $1 RETURNING *`,
        [txRes.rows[0].id]
      );

      await client.query("UPDATE books SET available = TRUE WHERE id = $1", [Number(bookId)]);
      await client.query("COMMIT");

      ok(res, rows[0], "Book returned successfully");
    } catch (err) {
      await client.query("ROLLBACK");
      throw err;
    } finally {
      client.release();
    }
  })
);

// GET /api/transactions
app.get(
  "/api/transactions",
  asyncHandler(async (req, res) => {
    const { bookId, memberId, active } = req.query;
    const conditions: string[] = [];
    const vals: unknown[] = [];

    if (bookId) { vals.push(Number(bookId)); conditions.push(`book_id = $${vals.length}`); }
    if (memberId) { vals.push(Number(memberId)); conditions.push(`member_id = $${vals.length}`); }
    if (active === "true") conditions.push("return_date IS NULL");
    if (active === "false") conditions.push("return_date IS NOT NULL");

    const where = conditions.length ? `WHERE ${conditions.join(" AND ")}` : "";
    const { rows } = await pool.query<Transaction>(
      `SELECT * FROM transactions ${where} ORDER BY created_at DESC`,
      vals
    );
    ok(res, rows, "Transactions fetched successfully");
  })
);

// GET /api/transactions/:id
app.get(
  "/api/transactions/:id",
  asyncHandler(async (req, res) => {
    const id = Number(req.params.id);
    if (!Number.isInteger(id) || id <= 0) return fail(res, "Invalid transaction ID", 400);

    const { rows } = await pool.query<Transaction>(
      "SELECT * FROM transactions WHERE id = $1",
      [id]
    );
    if (!rows[0]) return fail(res, "Transaction not found", 404);
    ok(res, rows[0], "Transaction fetched successfully");
  })
);

// ─────────────────────────────────────────────────────────────────────────────
// 404 & GLOBAL ERROR HANDLER
// ─────────────────────────────────────────────────────────────────────────────

app.use((_req: Request, res: Response) => {
  fail(res, "Route not found", 404);
});

app.use((err: unknown, _req: Request, res: Response, _next: NextFunction) => {
  const message = err instanceof Error ? err.message : "Internal server error";
  console.error("Unhandled error:", message);
  if (NODE_ENV !== "production") console.error(err);
  fail(res, NODE_ENV === "production" ? "Internal server error" : message, 500);
});

// ─────────────────────────────────────────────────────────────────────────────
// HELPERS
// ─────────────────────────────────────────────────────────────────────────────

function isUniqueViolation(err: unknown): boolean {
  return (
    typeof err === "object" &&
    err !== null &&
    "code" in err &&
    (err as { code: string }).code === "23505"
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// BOOT
// ─────────────────────────────────────────────────────────────────────────────

async function start(): Promise<void> {
  await initDB();
  app.listen(PORT, () => {
    console.log(`🚀  Server running on port ${PORT}  [${NODE_ENV}]`);
  });
}

start().catch((err) => {
  console.error("Failed to start server:", err);
  process.exit(1);
});
