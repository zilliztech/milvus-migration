package param

type JobResponse struct {
	JobId string `json:"jobId"`
}

func NewJobResponse(jobId string) *JobResponse {
	return &JobResponse{
		JobId: jobId,
	}
}
