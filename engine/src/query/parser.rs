// query/parser.rs

use crate::query::lexer::{LexError, Lexer, Token, TokenKind};
use anyhow::{Context, Result, bail};

/// AST definitions
#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    CreateTable {
        name: String,
        columns: Vec<(String, String)>,
    },
    Insert {
        table: String,
        columns: Vec<String>,
        values: Vec<Expr>,
    },
    Select {
        projections: Vec<Expr>,
        table: String,
        filter: Option<Expr>,
    },
    // Extendable: Update, Delete, etc.
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Column(String),
    Literal(Value),
    BinaryOp {
        left: Box<Expr>,
        op: BinaryOp,
        right: Box<Expr>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Int(i64),
    String(String),
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum BinaryOp {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    And,
    Or,
}

/// Recursive-descent parser
pub struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    /// Tokenize and build a new Parser
    pub fn new(src: &str) -> Result<Self> {
        let mut tokens = Vec::new();
        for item in Lexer::new(src) {
            match item {
                Ok(tok) => tokens.push(tok),
                Err(e) => bail!("Lex error: {:?}", e),
            }
        }
        Ok(Parser { tokens, pos: 0 })
    }

    /// Peek at the current token without consuming it.
    fn peek(&self) -> &Token {
        self.tokens.get(self.pos).unwrap_or(&Token {
            kind: TokenKind::EOF,
            line: 0,
            col: 0,
        })
    }

    /// Consume and return the current token (by cloning it).
    fn bump(&mut self) -> Token {
        let tok = self.peek().clone();
        self.pos += 1;
        tok
    }

    /// Expect the next token to be `kind`, else error.
    fn expect(&mut self, kind: TokenKind) -> Result<()> {
        let t = self.peek();
        if t.kind == kind {
            self.bump();
            Ok(())
        } else {
            bail!(
                "Expected {:?} at {}:{}, found {:?}",
                kind,
                t.line,
                t.col,
                t.kind
            );
        }
    }

    /// Parse one statement.
    pub fn parse_statement(&mut self) -> Result<Statement> {
        match &self.peek().kind {
            TokenKind::Create => self.parse_create_table(),
            TokenKind::Insert => self.parse_insert(),
            TokenKind::Select => self.parse_select(),
            other => bail!("Unexpected token {:?} at start of statement", other),
        }
    }

    fn parse_create_table(&mut self) -> Result<Statement> {
        self.expect(TokenKind::Create)?;
        self.expect(TokenKind::Table)?;
        let name = match self.bump().kind {
            TokenKind::Identifier(id) => id,
            _ => bail!("Expected table name"),
        };
        self.expect(TokenKind::LParen)?;
        let mut cols = Vec::new();
        loop {
            let col_name = match self.bump().kind {
                TokenKind::Identifier(id) => id,
                _ => bail!("Expected column name"),
            };
            let col_type = match self.bump().kind {
                TokenKind::Identifier(tp) => tp,
                _ => bail!("Expected type name"),
            };
            cols.push((col_name, col_type));
            if self.peek().kind == TokenKind::Comma {
                self.bump();
                continue;
            }
            break;
        }
        self.expect(TokenKind::RParen)?;
        self.expect(TokenKind::Semicolon)?;
        Ok(Statement::CreateTable {
            name,
            columns: cols,
        })
    }

    fn parse_insert(&mut self) -> Result<Statement> {
        self.expect(TokenKind::Insert)?;
        self.expect(TokenKind::Into)?;
        let table = match self.bump().kind {
            TokenKind::Identifier(id) => id,
            _ => bail!("Expected table name"),
        };
        self.expect(TokenKind::LParen)?;
        let mut cols = Vec::new();
        loop {
            match &self.bump().kind {
                TokenKind::Identifier(id) => cols.push(id.clone()),
                _ => bail!("Expected column name"),
            };
            if self.peek().kind == TokenKind::Comma {
                self.bump();
                continue;
            }
            break;
        }
        self.expect(TokenKind::RParen)?;
        self.expect(TokenKind::Values)?;
        self.expect(TokenKind::LParen)?;
        let mut vals = Vec::new();
        loop {
            vals.push(self.parse_expr()?);
            if self.peek().kind == TokenKind::Comma {
                self.bump();
                continue;
            }
            break;
        }
        self.expect(TokenKind::RParen)?;
        self.expect(TokenKind::Semicolon)?;
        Ok(Statement::Insert {
            table,
            columns: cols,
            values: vals,
        })
    }

    fn parse_select(&mut self) -> Result<Statement> {
        self.expect(TokenKind::Select)?;
        let mut projections = Vec::new();
        loop {
            projections.push(self.parse_expr()?);
            if self.peek().kind == TokenKind::Comma {
                self.bump();
                continue;
            }
            break;
        }
        self.expect(TokenKind::From)?;
        let table = match self.bump().kind {
            TokenKind::Identifier(id) => id,
            _ => bail!("Expected table name"),
        };
        let filter = if self.peek().kind == TokenKind::Where {
            self.bump();
            Some(self.parse_expr()?)
        } else {
            None
        };
        self.expect(TokenKind::Semicolon)?;
        Ok(Statement::Select {
            projections,
            table,
            filter,
        })
    }

    fn parse_expr(&mut self) -> Result<Expr> {
        self.parse_binary_op(0)
    }

    fn parse_binary_op(&mut self, min_prec: u8) -> Result<Expr> {
        let mut left = self.parse_primary()?;
        while let Some((op, prec)) = self.peek_op_prec() {
            if prec < min_prec {
                break;
            }
            let op = op;
            self.bump();
            let right = self.parse_binary_op(prec + 1)?;
            left = Expr::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn peek_op_prec(&self) -> Option<(BinaryOp, u8)> {
        match self.peek().kind {
            TokenKind::Eq => Some((BinaryOp::Eq, 10)),
            TokenKind::NotEq => Some((BinaryOp::NotEq, 10)),
            TokenKind::Lt => Some((BinaryOp::Lt, 10)),
            TokenKind::LtEq => Some((BinaryOp::LtEq, 10)),
            TokenKind::Gt => Some((BinaryOp::Gt, 10)),
            TokenKind::GtEq => Some((BinaryOp::GtEq, 10)),
            TokenKind::And => Some((BinaryOp::And, 5)),
            TokenKind::Or => Some((BinaryOp::Or, 4)),
            _ => None,
        }
    }

    fn parse_primary(&mut self) -> Result<Expr> {
        match &self.peek().kind {
            TokenKind::Identifier(id) => {
                let name = id.clone();
                self.bump();
                Ok(Expr::Column(name))
            }
            TokenKind::IntLiteral(v) => {
                let val = *v;
                self.bump();
                Ok(Expr::Literal(Value::Int(val)))
            }
            TokenKind::StringLiteral(s) => {
                let val = s.clone();
                self.bump();
                Ok(Expr::Literal(Value::String(val)))
            }
            TokenKind::LParen => {
                self.bump();
                let expr = self.parse_expr()?;
                self.expect(TokenKind::RParen)?;
                Ok(expr)
            }
            other => bail!("Unexpected token in expression: {:?}", other),
        }
    }
}
