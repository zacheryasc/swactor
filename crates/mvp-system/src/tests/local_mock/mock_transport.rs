use mvp_system::orchestration::run_plan as plan;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MockObjectKind {
    Token,
    Activation,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MockObject {
    pub edge_id: plan::EdgeId,
    pub object_id: u64,
    pub sequence: u64,
    pub kind: MockObjectKind,
    pub token_id: Option<u32>,
    pub eos: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Delivery {
    pub edge_id: plan::EdgeId,
    pub object_id: u64,
    pub sequence: u64,
    pub kind: MockObjectKind,
}

#[derive(Default)]
pub struct MockTransport {
    deliveries: Vec<Delivery>,
}

impl MockTransport {
    pub fn deliver(&mut self, object: MockObject) -> MockObject {
        self.deliveries.push(Delivery {
            edge_id: object.edge_id,
            object_id: object.object_id,
            sequence: object.sequence,
            kind: object.kind,
        });
        object
    }

    pub fn deliveries(&self) -> &[Delivery] {
        &self.deliveries
    }
}
