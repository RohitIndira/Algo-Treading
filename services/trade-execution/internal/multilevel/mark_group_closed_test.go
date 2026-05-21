package multilevel

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/repository"
)

// fakeMLRepo is a partial OrderRepository fake. It embeds the interface (nil) so it
// satisfies the type; only the methods exercised by the test are implemented. Any
// other method call would nil-panic, which is the desired guard for a focused test.
type fakeMLRepo struct {
	repository.OrderRepository
	supersedeCalls []uuid.UUID
	supersedeErr   error
}

func (f *fakeMLRepo) SupersedeMultiLevelLevels(ctx context.Context, entryOrderID uuid.UUID) error {
	f.supersedeCalls = append(f.supersedeCalls, entryOrderID)
	return f.supersedeErr
}

// TestMarkGroupClosedExternally_NoGroup_SupersedesLevels verifies the core of the
// Option-C fix: when the paper monitor closes a position externally and no in-memory
// ML group is left, MarkGroupClosedExternally still reconciles the DB by superseding
// the un-fired ladder levels. The old CancelGroup path did NOT do this, which left
// levels PENDING until they were mislabelled CANCELLED at strategy deactivation.
func TestMarkGroupClosedExternally_NoGroup_SupersedesLevels(t *testing.T) {
	repo := &fakeMLRepo{}
	m := &Manager{repo: repo, logger: zap.NewNop()}

	entryOrderID := uuid.New()
	m.MarkGroupClosedExternally(context.Background(), entryOrderID)

	if len(repo.supersedeCalls) != 1 {
		t.Fatalf("expected SupersedeMultiLevelLevels to be called once, got %d calls", len(repo.supersedeCalls))
	}
	if repo.supersedeCalls[0] != entryOrderID {
		t.Fatalf("SupersedeMultiLevelLevels called with %s, want %s", repo.supersedeCalls[0], entryOrderID)
	}
}

// TestMarkGroupClosedExternally_RepoError_DoesNotPanic verifies the method tolerates
// a DB error from SupersedeMultiLevelLevels — it must log and return, never panic,
// so a transient DB failure cannot abort an in-progress position exit.
func TestMarkGroupClosedExternally_RepoError_DoesNotPanic(t *testing.T) {
	repo := &fakeMLRepo{supersedeErr: errors.New("db unavailable")}
	m := &Manager{repo: repo, logger: zap.NewNop()}

	// Must not panic.
	m.MarkGroupClosedExternally(context.Background(), uuid.New())

	if len(repo.supersedeCalls) != 1 {
		t.Fatalf("expected SupersedeMultiLevelLevels to be attempted once, got %d", len(repo.supersedeCalls))
	}
}
