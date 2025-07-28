struct User {
    name: String,
    age: i32,
}

fn main() {
    let u1 = User {
        name: "123".to_string(),
        age: 20,
    };
    let u2 = &u1;
    let v1 = &u2.name;
}
