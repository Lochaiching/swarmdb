

func TestEnumeratorNext(t *testing.T) {
	// seeking within 3 keys: 10, 20, 30
	table := []struct {
		k    int
		hit  bool
		keys []int
	}{
		{5, false, []int{10, 20, 30}},
		{10, true, []int{10, 20, 30}},
		{15, false, []int{20, 30}},
		{20, true, []int{20, 30}},
		{25, false, []int{30}},
		{30, true, []int{30}},
		{35, false, []int{}},
	}

	for i, test := range table {
		up := test.keys
		r := TreeNew(cmp)

		r.Set(10, 100)
		r.Set(20, 200)
		r.Set(30, 300)

		for verChange := 0; verChange < 16; verChange++ {
			en, hit := r.Seek(test.k)

			if g, e := hit, test.hit; g != e {
				t.Fatal(i, g, e)
			}

			j := 0
			for {
				if verChange&(1<<uint(j)) != 0 {
					r.Set(20, 200)
				}

				k, v, err := en.Next()
				if err != nil {
					if err != io.EOF {
						t.Fatal(i, err)
					}

					break
				}

				if j >= len(up) {
					t.Fatal(i, j, verChange)
				}

				if g, e := k.(int), up[j]; g != e {
					t.Fatal(i, j, verChange, g, e)
				}

				if g, e := v.(int), 10*up[j]; g != e {
					t.Fatal(i, g, e)
				}

				j++

			}

			if g, e := j, len(up); g != e {
				t.Fatal(i, j, g, e)
			}
		}

	}
}

func TestEnumeratorPrev(t *testing.T) {
	// seeking within 3 keys: 10, 20, 30
	table := []struct {
		k    int
		hit  bool
		keys []int
	}{
		{5, false, []int{}},
		{10, true, []int{10}},
		{15, false, []int{10}},
		{20, true, []int{20, 10}},
		{25, false, []int{20, 10}},
		{30, true, []int{30, 20, 10}},
		{35, false, []int{30, 20, 10}},
	}

	for i, test := range table {
		dn := test.keys
		r := TreeNew(cmp)

		r.Set(10, 100)
		r.Set(20, 200)
		r.Set(30, 300)

		for verChange := 0; verChange < 16; verChange++ {
			en, hit := r.Seek(test.k)

			if g, e := hit, test.hit; g != e {
				t.Fatal(i, g, e)
			}

			j := 0
			for {
				if verChange&(1<<uint(j)) != 0 {
					r.Set(20, 200)
				}

				k, v, err := en.Prev()
				if err != nil {
					if err != io.EOF {
						t.Fatal(i, err)
					}

					break
				}

				if j >= len(dn) {
					t.Fatal(i, j, verChange)
				}

				if g, e := k.(int), dn[j]; g != e {
					t.Fatal(i, j, verChange, g, e)
				}

				if g, e := v.(int), 10*dn[j]; g != e {
					t.Fatal(i, g, e)
				}

				j++

			}

			if g, e := j, len(dn); g != e {
				t.Fatal(i, j, g, e)
			}
		}

	}
}

func TestEnumeratorPrevSanity(t *testing.T) {
	// seeking within 3 keys: 10, 20, 30
	table := []struct {
		k      int
		hit    bool
		kOut   interface{}
		vOut   interface{}
		errOut error
	}{
		{10, true, 10, 100, nil},
		{20, true, 20, 200, nil},
		{30, true, 30, 300, nil},
		{35, false, 30, 300, nil},
		{25, false, 20, 200, nil},
		{15, false, 10, 100, nil},
		{5, false, nil, nil, io.EOF},
	}

	for i, test := range table {
		r := TreeNew(cmp)

		r.Set(10, 100)
		r.Set(20, 200)
		r.Set(30, 300)

		en, hit := r.Seek(test.k)

		if g, e := hit, test.hit; g != e {
			t.Fatal(i, g, e)
		}

		k, v, err := en.Prev()

		if g, e := err, test.errOut; g != e {
			t.Fatal(i, g, e)
		}
		if g, e := k, test.kOut; g != e {
			t.Fatal(i, g, e)
		}
		if g, e := v, test.vOut; g != e {
			t.Fatal(i, g, e)
		}
	}
}

func TestSeekFirst0(t *testing.T) {
	b := TreeNew(cmp)
	_, err := b.SeekFirst()
	if g, e := err, io.EOF; g != e {
		t.Fatal(g, e)
	}
}

func TestSeekFirst1(t *testing.T) {
	b := TreeNew(cmp)
	b.Set(1, 10)
	en, err := b.SeekFirst()
	if err != nil {
		t.Fatal(err)
	}

	k, v, err := en.Next()
	if k != 1 || v != 10 || err != nil {
		t.Fatal(k, v, err)
	}

	k, v, err = en.Next()
	if err == nil {
		t.Fatal(k, v, err)
	}
}

func TestSeekFirst2(t *testing.T) {
	b := TreeNew(cmp)
	b.Set(1, 10)
	b.Set(2, 20)
	en, err := b.SeekFirst()
	if err != nil {
		t.Fatal(err)
	}

	k, v, err := en.Next()
	if k != 1 || v != 10 || err != nil {
		t.Fatal(k, v, err)
	}

	k, v, err = en.Next()
	if k != 2 || v != 20 || err != nil {
		t.Fatal(k, v, err)
	}

	k, v, err = en.Next()
	if err == nil {
		t.Fatal(k, v, err)
	}
}

func TestSeekFirst3(t *testing.T) {
	b := TreeNew(cmp)
	b.Set(2, 20)
	b.Set(3, 30)
	b.Set(1, 10)
	en, err := b.SeekFirst()
	if err != nil {
		t.Fatal(err)
	}

	k, v, err := en.Next()
	if k != 1 || v != 10 || err != nil {
		t.Fatal(k, v, err)
	}

	k, v, err = en.Next()
	if k != 2 || v != 20 || err != nil {
		t.Fatal(k, v, err)
	}

	k, v, err = en.Next()
	if k != 3 || v != 30 || err != nil {
		t.Fatal(k, v, err)
	}

	k, v, err = en.Next()
	if err == nil {
		t.Fatal(k, v, err)
	}
}

func TestSeekLast0(t *testing.T) {
	b := TreeNew(cmp)
	_, err := b.SeekLast()
	if g, e := err, io.EOF; g != e {
		t.Fatal(g, e)
	}
}

func TestSeekLast1(t *testing.T) {
	b := TreeNew(cmp)
	b.Set(1, 10)
	en, err := b.SeekLast()
	if err != nil {
		t.Fatal(err)
	}

	k, v, err := en.Prev()
	if k != 1 || v != 10 || err != nil {
		t.Fatal(k, v, err)
	}

	k, v, err = en.Prev()
	if err == nil {
		t.Fatal(k, v, err)
	}
}

func TestSeekLast2(t *testing.T) {
	b := TreeNew(cmp)
	b.Set(1, 10)
	b.Set(2, 20)
	en, err := b.SeekLast()
	if err != nil {
		t.Fatal(err)
	}

	k, v, err := en.Prev()
	if k != 2 || v != 20 || err != nil {
		t.Fatal(k, v, err)
	}

	k, v, err = en.Prev()
	if k != 1 || v != 10 || err != nil {
		t.Fatal(k, v, err)
	}

	k, v, err = en.Prev()
	if err == nil {
		t.Fatal(k, v, err)
	}
}

func TestSeekLast3(t *testing.T) {
	b := TreeNew(cmp)
	b.Set(2, 20)
	b.Set(3, 30)
	b.Set(1, 10)
	en, err := b.SeekLast()
	if err != nil {
		t.Fatal(err)
	}

	k, v, err := en.Prev()
	if k != 3 || v != 30 || err != nil {
		t.Fatal(k, v, err)
	}

	k, v, err = en.Prev()
	if k != 2 || v != 20 || err != nil {
		t.Fatal(k, v, err)
	}

	k, v, err = en.Prev()
	if k != 1 || v != 10 || err != nil {
		t.Fatal(k, v, err)
	}

	k, v, err = en.Prev()
	if err == nil {
		t.Fatal(k, v, err)
	}
}

func TestPut(t *testing.T) {
	tab := []struct {
		pre    []int // even index: K, odd index: V
		newK   int   // Put(newK, ...
		oldV   int   // Put()->oldV
		exists bool  // upd(exists)
		write  bool  // upd()->write
		post   []int // even index: K, odd index: V
	}{
		// 0
		{
			[]int{},
			1, 0, false, false,
			[]int{},
		},
		{
			[]int{},
			1, 0, false, true,
			[]int{1, -1},
		},
		{
			[]int{1, 10},
			0, 0, false, false,
			[]int{1, 10},
		},
		{
			[]int{1, 10},
			0, 0, false, true,
			[]int{0, -1, 1, 10},
		},
		{
			[]int{1, 10},
			1, 10, true, false,
			[]int{1, 10},
		},

		// 5
		{
			[]int{1, 10},
			1, 10, true, true,
			[]int{1, -1},
		},
		{
			[]int{1, 10},
			2, 0, false, false,
			[]int{1, 10},
		},
		{
			[]int{1, 10},
			2, 0, false, true,
			[]int{1, 10, 2, -1},
		},
	}

	for iTest, test := range tab {
		tr := TreeNew(cmp)
		for i := 0; i < len(test.pre); i += 2 {
			k, v := test.pre[i], test.pre[i+1]
			tr.Set(k, v)
		}

		oldV, written := tr.Put(test.newK, func(old interface{}, exists bool) (newV interface{}, write bool) {
			if g, e := exists, test.exists; g != e {
				t.Fatal(iTest, g, e)
			}

			if exists {
				if g, e := old.(int), test.oldV; g != e {
					t.Fatal(iTest, g, e)
				}
			}
			return -1, test.write
		})
		if test.exists {
			if g, e := oldV.(int), test.oldV; g != e {
				t.Fatal(iTest, g, e)
			}
		}

		if g, e := written, test.write; g != e {
			t.Fatal(iTest, g, e)
		}

		n := len(test.post)
		en, err := tr.SeekFirst()
		if err != nil {
			if n == 0 && err == io.EOF {
				continue
			}

			t.Fatal(iTest, err)
		}

		for i := 0; i < len(test.post); i += 2 {
			k, v, err := en.Next()
			if err != nil {
				t.Fatal(iTest, err)
			}

			if g, e := k.(int), test.post[i]; g != e {
				t.Fatal(iTest, g, e)
			}

			if g, e := v.(int), test.post[i+1]; g != e {
				t.Fatal(iTest, g, e)
			}
		}

		_, _, err = en.Next()
		if g, e := err, io.EOF; g != e {
			t.Fatal(iTest, g, e)
		}
	}
}

func TestSeek(t *testing.T) {
	const N = 1 << 13
	tr := TreeNew(cmp)
	for i := 0; i < N; i++ {
		k := 2*i + 1
		tr.Set(k, nil)
	}
	for i := 0; i < N; i++ {
		k := 2 * i
		e, ok := tr.Seek(k)
		if ok {
			t.Fatal(k)
		}

		for j := i; j < N; j++ {
			k2, _, err := e.Next()
			if err != nil {
				t.Fatal(k, err)
			}

			if g, e := k2, 2*j+1; g != e {
				t.Fatal(j, g, e)
			}
		}

		_, _, err := e.Next()
		if err != io.EOF {
			t.Fatalf("expected io.EOF, got %v", err)
		}
	}
}

func TestPR4(t *testing.T) {
	tr := TreeNew(cmp)
	for i := 0; i < 2*kd+1; i++ {
		k := 1000 * i
		tr.Set(k, nil)
	}
	tr.Delete(1000 * kd)
	for i := 0; i < kd; i++ {
		tr.Set(1000*(kd+1)-1-i, nil)
	}
	k := 1000*(kd+1) - 1 - kd
	tr.Set(k, nil)
	if _, ok := tr.Get(k); !ok {
		t.Fatalf("key lost: %v", k)
	}
}

