FROM rust

# Set destination for COPY
#WORKDIR /app

COPY . .
#COPY .env /app/

RUN cargo build --release

CMD ["target/release/options_analyzer"]
