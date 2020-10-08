use crate::graphql_json::GraphQLJson;
use juniper::graphql_object;
use juniper::{EmptyMutation, EmptySubscription, FieldError, GraphQLEnum, RootNode};

#[derive(Clone, Copy, Debug)]
pub struct Context;

impl juniper::Context for Context {}

#[derive(Clone, Copy, Debug, GraphQLEnum)]
pub enum UserKind {
    Admin,
    User,
    Guest,
}

#[derive(Clone, Debug)]
pub struct User {
    pub id: i32,
    pub kind: UserKind,
    pub name: String,
    pub json: GraphQLJson,
}

#[graphql_object(Context = Context)]
impl User {
    fn id(&self) -> i32 {
        self.id
    }

    fn kind(&self) -> UserKind {
        self.kind
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn json(&self) -> &GraphQLJson {
        &self.json
    }

    async fn friends(&self) -> Vec<User> {
        vec![]
    }
}

#[derive(Clone, Copy, Debug)]
pub struct QueryRoot;

#[graphql_object(Context = Context)]
impl QueryRoot {
    async fn users() -> Vec<User> {
        vec![
            User {
                id: 1,
                kind: UserKind::Admin,
                name: "user 1".into(),
                json: GraphQLJson(json!({})),
            },
            User {
                id: 2,
                kind: UserKind::Admin,
                name: "user 2".into(),
                json: GraphQLJson(json!([])),
            },
            User {
                id: 3,
                kind: UserKind::Admin,
                name: "user 3".into(),
                json: GraphQLJson(json!({"int": 10, "arr": [1, "a", "b", "c"]})),
            },
            User {
                id: 4,
                kind: UserKind::Admin,
                name: "user 3".into(),
                json: GraphQLJson(json!({"int": 11, "arr": [1.1, null, {}, "c"]})),
            },
        ]
    }

    /// Fetch a URL and return the response body text.
    async fn request(url: String) -> Result<String, FieldError> {
        Ok(reqwest::get(&url).await?.text().await?)
    }
}

pub type Schema = RootNode<'static, QueryRoot, EmptyMutation<Context>, EmptySubscription<Context>>;

pub fn schema() -> Schema {
    Schema::new(
        QueryRoot,
        EmptyMutation::<Context>::new(),
        EmptySubscription::<Context>::new(),
    )
}
