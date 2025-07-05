

use std::iter::Peekable;
use std::str::Chars;


#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TokenKind {
    
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
    
    Identifier(String),
    IntLiteral(i64),
    StringLiteral(String),
    
    Eq,    
    NotEq, 
    Lt,    
    LtEq,  
    Gt,    
    GtEq,  
    Plus,  
    Minus, 
    Star,  
    Slash, 
    
    Comma,     
    Semicolon, 
    LParen,    
    RParen,    
    
    EOF,
}


#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Token {
    pub kind: TokenKind,
    pub line: usize,
    pub col: usize,
}


#[derive(Debug, Clone)]
pub enum LexError {
    UnexpectedChar(char, usize, usize),
    UnterminatedString(usize, usize),
    InvalidNumber(String, usize, usize),
}


pub struct Lexer<'src> {
    input: Peekable<Chars<'src>>,
    src: &'src str,
    
    idx: usize,
    line: usize,
    col: usize,
}

impl<'src> Lexer<'src> {
    
    pub fn new(src: &'src str) -> Self {
        Lexer {
            input: src.chars().peekable(),
            src,
            idx: 0,
            line: 1,
            col: 1,
        }
    }

    
    fn peek_char(&mut self) -> Option<char> {
        self.input.peek().copied()
    }

    
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

    
    fn skip_whitespace_and_comments(&mut self) {
        loop {
            
            while matches!(self.peek_char(), Some(c) if c.is_whitespace()) {
                self.next_char();
            }
            
            if self.peek_char() == Some('-') {
                
                let mut iter = self.input.clone();
                if iter.next() == Some('-') && iter.next() == Some('-') {
                    
                    self.next_char();
                    self.next_char();
                    
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

    
    fn read_identifier_or_keyword(&mut self) -> String {
        let start_idx = self.idx;
        while matches!(self.peek_char(), Some(c) if c.is_ascii_alphanumeric() || c == '_') {
            self.next_char();
        }
        
        self.src[start_idx..self.idx].to_ascii_uppercase()
    }

    
    fn read_number(&mut self) -> Result<String, LexError> {
        let start_idx = self.idx;
        while matches!(self.peek_char(), Some(c) if c.is_ascii_digit()) {
            self.next_char();
        }
        Ok(self.src[start_idx..self.idx].to_string())
    }

    
    fn read_string(&mut self) -> Result<String, LexError> {
        
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

    
    fn next_token(&mut self) -> Result<Token, LexError> {
        self.skip_whitespace_and_comments();
        let (line, col) = (self.line, self.col);

        let tok = match self.next_char() {
            Some(c) => match c {
                
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
                    
                    self.idx -= c.len_utf8();
                    self.col -= 1;
                    self.input.next_back(); 
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
                    
                    self.idx -= c.len_utf8();
                    self.col -= 1;
                    self.input.next_back();
                    let ident = self.read_identifier_or_keyword();
                    
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
