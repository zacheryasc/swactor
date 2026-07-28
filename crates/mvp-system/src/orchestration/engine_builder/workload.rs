use super::engine::ClusterHandle;

pub trait WorkloadAdapter {
    type Input;
    type Output;
    type Error;

    fn submit(
        &self,
        cluster: &mut ClusterHandle,
        input: Self::Input,
    ) -> Result<Self::Output, Self::Error>;
}
