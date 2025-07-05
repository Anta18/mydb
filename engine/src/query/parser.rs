

use crate::query::lexer::{Lexer, Token, TokenKind};
use anyhow::{Result, anyhow, bail};


#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    CreateTable {
        name: String,
        columns: Vec<(String, String)>,
    },
    CreateIndex {
        index_name: String,
        table: String,
        column: String,
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


pub struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    
    pub fn new(src: &str) -> Result<Self> {
        let mut tokens = Vec::new();
        for item in Lexer::new(src) {
            
            let tok = item.map_err(|e| anyhow!("Lex error: {:?}", e))?;
            tokens.push(tok);
        }
        Ok(Parser { tokens, pos: 0 })
    }

    fn peek(&self) -> &Token {
        self.tokens.get(self.pos).unwrap_or(&Token {
            kind: TokenKind::EOF,
            line: 0,
            col: 0,
        })
    }

    fn bump(&mut self) -> Token {
        let t = self.peek().clone();
        self.pos += 1;
        t
    }

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

    
    pub fn parse_statement(&mut self) -> Result<Statement> {
        match &self.peek().kind {
            TokenKind::Create => {
                
                if let Some(tok) = self.tokens.get(self.pos + 1) {
                    if let TokenKind::Identifier(ref s) = tok.kind {
                        if s.eq_ignore_ascii_case("INDEX") {
                            return self.parse_create_index();
                        }
                    }
                }
                self.parse_create_table()
            }
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
            } else {
                break;
            }
        }
        self.expect(TokenKind::RParen)?;
        self.expect(TokenKind::Semicolon)?;
        Ok(Statement::CreateTable {
            name,
            columns: cols,
        })
    }

    fn parse_create_index(&mut self) -> Result<Statement> {
        self.expect(TokenKind::Create)?;
        
        if let TokenKind::Identifier(ref s) = self.peek().kind {
            if s.eq_ignore_ascii_case("INDEX") {
                self.bump();
            } else {
                bail!("Expected INDEX");
            }
        } else {
            bail!("Expected INDEX");
        }
        let index_name = match self.bump().kind {
            TokenKind::Identifier(id) => id,
            _ => bail!("Expected index name"),
        };
        
        if let TokenKind::Identifier(ref s) = self.peek().kind {
            if s.eq_ignore_ascii_case("ON") {
                self.bump();
            } else {
                bail!("Expected ON");
            }
        } else {
            bail!("Expected ON");
        }
        let table = match self.bump().kind {
            TokenKind::Identifier(id) => id,
            _ => bail!("Expected table name"),
        };
        self.expect(TokenKind::LParen)?;
        let column = match self.bump().kind {
            TokenKind::Identifier(id) => id,
            _ => bail!("Expected column name"),
        };
        self.expect(TokenKind::RParen)?;
        self.expect(TokenKind::Semicolon)?;
        Ok(Statement::CreateIndex {
            index_name,
            table,
            column,
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
            }
            if self.peek().kind == TokenKind::Comma {
                self.bump();
            } else {
                break;
            }
        }
        self.expect(TokenKind::RParen)?;
        self.expect(TokenKind::Values)?;
        self.expect(TokenKind::LParen)?;
        let mut vals = Vec::new();
        loop {
            vals.push(self.parse_expr()?);
            if self.peek().kind == TokenKind::Comma {
                self.bump();
            } else {
                break;
            }
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
            } else {
                break;
            }
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
        use BinaryOp::*;
        match self.peek().kind {
            TokenKind::Eq => Some((Eq, 10)),
            TokenKind::NotEq => Some((NotEq, 10)),
            TokenKind::Lt => Some((Lt, 10)),
            TokenKind::LtEq => Some((LtEq, 10)),
            TokenKind::Gt => Some((Gt, 10)),
            TokenKind::GtEq => Some((GtEq, 10)),
            TokenKind::And => Some((And, 5)),
            TokenKind::Or => Some((Or, 4)),
            _ => None,
        }
    }

    fn parse_primary(&mut self) -> Result<Expr> {
        match &self.peek().kind {
            TokenKind::Identifier(id) => {
                let c = id.clone();
                self.bump();
                Ok(Expr::Column(c))
            }
            TokenKind::IntLiteral(v) => {
                let i = *v;
                self.bump();
                Ok(Expr::Literal(Value::Int(i)))
            }
            TokenKind::StringLiteral(s) => {
                let s2 = s.clone();
                self.bump();
                Ok(Expr::Literal(Value::String(s2)))
            }
            TokenKind::LParen => {
                self.bump();
                let e = self.parse_expr()?;
                self.expect(TokenKind::RParen)?;
                Ok(e)
            }
            other => bail!("Unexpected token in expression: {:?}", other),
        }
    }
}
