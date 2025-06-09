// src/sql/lexer.rs

use std::iter::Peekable;
use std::str::Chars;

/// All token types in our SQL subset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TokenKind {
    // Keywords
    Select,
    Insert,
    Update,
    Delete,
    From,
    Where,
    And,
    Or,
    Create,
    Table,
    Into,
    Values,
    // Identifiers & Literals
    Identifier(String),
    IntLiteral(i64),
    StringLiteral(String),
    // Operators
    Eq,    // =
    NotEq, // <>
    Lt,    // <
    LtEq,  // <=
    Gt,    // >
    GtEq,  // >=
    Plus,  // +
    Minus, // -
    Star,  // *
    Slash, // /
    // Punctuation
    Comma,     // ,
    Semicolon, // ;
    LParen,    // (
    RParen,    // )
    // End of input
    EOF,
}

/// A token with its kind and location.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Token {
    pub kind: TokenKind,
    pub line: usize,
    pub col: usize,
}

/// Lexing errors, with position.
#[derive(Debug, Clone)]
pub enum LexError {
    UnexpectedChar(char, usize, usize),
    UnterminatedString(usize, usize),
    InvalidNumber(String, usize, usize),
}

/// The SQL lexer: an iterator over `Token` or `LexError`.
pub struct Lexer<'src> {
    input: Peekable<Chars<'src>>,
    src: &'src str,
    /// Current absolute index into `src`
    idx: usize,
    line: usize,
    col: usize,
}

impl<'src> Lexer<'src> {
    /// Create a new lexer from input SQL text.
    pub fn new(src: &'src str) -> Self {
        Lexer {
            input: src.chars().peekable(),
            src,
            idx: 0,
            line: 1,
            col: 1,
        }
    }

    /// Peek next character without consuming.
    fn peek_char(&mut self) -> Option<char> {
        self.input.peek().copied()
    }

    /// Consume and return next character.
    fn next_char(&mut self) -> Option<char> {
        let c = self.input.next()?;
        self.idx += c.len_utf8();
        if c == '\n' {
            self.line += 1;
            self.col = 1;
        } else {
            self.col += 1;
        }
        Some(c)
    }

    /// Skip whitespace and comments (`-- ... \n`).
    fn skip_whitespace_and_comments(&mut self) {
        loop {
            // Skip spaces, tabs, newlines
            while matches!(self.peek_char(), Some(c) if c.is_whitespace()) {
                self.next_char();
            }
            // Skip line comments
            if self.peek_char() == Some('-') {
                // look ahead for "--"
                let mut iter = self.input.clone();
                if iter.next() == Some('-') && iter.next() == Some('-') {
                    // consume "--"
                    self.next_char();
                    self.next_char();
                    // consume until newline or EOF
                    while let Some(c) = self.peek_char() {
                        if c == '\n' {
                            break;
                        }
                        self.next_char();
                    }
                    continue;
                }
            }
            break;
        }
    }

    /// Try to read an identifier or keyword.
    fn read_identifier_or_keyword(&mut self) -> String {
        let start_idx = self.idx;
        while matches!(self.peek_char(), Some(c) if c.is_ascii_alphanumeric() || c == '_') {
            self.next_char();
        }
        // slice out the identifier
        self.src[start_idx..self.idx].to_ascii_uppercase()
    }

    /// Try to read a decimal integer literal.
    fn read_number(&mut self) -> Result<String, LexError> {
        let start_idx = self.idx;
        while matches!(self.peek_char(), Some(c) if c.is_ascii_digit()) {
            self.next_char();
        }
        Ok(self.src[start_idx..self.idx].to_string())
    }

    /// Read a `'...'` string literal (single quotes).
    fn read_string(&mut self) -> Result<String, LexError> {
        // assume opening ' has been consumed
        let mut result = String::new();
        loop {
            match self.next_char() {
                Some('\'') => break,
                Some(c) => result.push(c),
                None => {
                    return Err(LexError::UnterminatedString(self.line, self.col));
                }
            }
        }
        Ok(result)
    }

    /// Main lexing function: produce next token or error.
    fn next_token(&mut self) -> Result<Token, LexError> {
        self.skip_whitespace_and_comments();
        let (line, col) = (self.line, self.col);

        let tok = match self.next_char() {
            Some(c) => match c {
                // Single-character tokens
                ',' => TokenKind::Comma,
                ';' => TokenKind::Semicolon,
                '(' => TokenKind::LParen,
                ')' => TokenKind::RParen,
                '+' => TokenKind::Plus,
                '-' => TokenKind::Minus,
                '*' => TokenKind::Star,
                '/' => TokenKind::Slash,
                '=' => TokenKind::Eq,
                '<' => {
                    if self.peek_char() == Some('=') {
                        self.next_char();
                        TokenKind::LtEq
                    } else if self.peek_char() == Some('>') {
                        self.next_char();
                        TokenKind::NotEq
                    } else {
                        TokenKind::Lt
                    }
                }
                '>' => {
                    if self.peek_char() == Some('=') {
                        self.next_char();
                        TokenKind::GtEq
                    } else {
                        TokenKind::Gt
                    }
                }
                '\'' => {
                    let s = self.read_string()?;
                    return Ok(Token {
                        kind: TokenKind::StringLiteral(s),
                        line,
                        col,
                    });
                }
                c if c.is_ascii_digit() => {
                    // back up one char in indices
                    self.idx -= c.len_utf8();
                    self.col -= 1;
                    self.input.next_back(); // un-consume
                    let num_str = self.read_number().map_err(|e| e)?;
                    match num_str.parse::<i64>() {
                        Ok(v) => {
                            return Ok(Token {
                                kind: TokenKind::IntLiteral(v),
                                line,
                                col,
                            });
                        }
                        Err(_) => return Err(LexError::InvalidNumber(num_str, line, col)),
                    }
                }
                c if c.is_ascii_alphabetic() || c == '_' => {
                    // back up one char
                    self.idx -= c.len_utf8();
                    self.col -= 1;
                    self.input.next_back();
                    let ident = self.read_identifier_or_keyword();
                    // map to keyword or identifier
                    return Ok(Token {
                        kind: match ident.as_str() {
                            "SELECT" => TokenKind::Select,
                            "INSERT" => TokenKind::Insert,
                            "UPDATE" => TokenKind::Update,
                            "DELETE" => TokenKind::Delete,
                            "FROM" => TokenKind::From,
                            "WHERE" => TokenKind::Where,
                            "AND" => TokenKind::And,
                            "OR" => TokenKind::Or,
                            "CREATE" => TokenKind::Create,
                            "TABLE" => TokenKind::Table,
                            "INTO" => TokenKind::Into,
                            "VALUES" => TokenKind::Values,
                            other => TokenKind::Identifier(other.to_string()),
                        },
                        line,
                        col,
                    });
                }
                other => return Err(LexError::UnexpectedChar(other, line, col)),
            },
            None => TokenKind::EOF,
        };

        Ok(Token {
            kind: tok,
            line,
            col,
        })
    }
}

impl<'src> Iterator for Lexer<'src> {
    type Item = Result<Token, LexError>;
    fn next(&mut self) -> Option<Self::Item> {
        match self.next_token() {
            Ok(token) => {
                if token.kind == TokenKind::EOF {
                    Some(Ok(token))
                } else {
                    Some(Ok(token))
                }
            }
            Err(e) => Some(Err(e)),
        }
    }
}
