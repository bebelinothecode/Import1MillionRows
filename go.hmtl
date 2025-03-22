@extends('layouts.app')

@section('content')
    <div class="roles-permissions">
        <div class="flex items-center justify-between mb-6">
            <div>
                <h2 class="text-gray-700 uppercase font-bold">Teachers</h2>
            </div>
            {{-- @hasanyrole('Admin|rector|AsstAccountfrontdesk') --}}
            <div class="flex flex-wrap items-center">
                <a href="{{ route('teacher.create') }}" class="bg-gray-200 text-gray-700 text-sm uppercase py-2 px-4 flex items-center rounded">
                    <svg class="w-3 h-3 fill-current" aria-hidden="true" focusable="false" data-prefix="fas" data-icon="plus" class="svg-inline--fa fa-plus fa-w-14" role="img" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 448 512"><path fill="currentColor" d="M416 208H272V64c0-17.67-14.33-32-32-32h-32c-17.67 0-32 14.33-32 32v144H32c-17.67 0-32 14.33-32 32v32c0 17.67 14.33 32 32 32h144v144c0 17.67 14.33 32 32 32h32c17.67 0 32-14.33 32-32V304h144c17.67 0 32-14.33 32-32v-32c0-17.67-14.33-32-32-32z"></path></svg>
                    <span class="ml-2 text-xs font-semibold">Add Teacher</span>
                </a>
            </div>
            {{-- @endhasanyrole --}}
        </div>
        <div class="mt-6 bg-white rounded-lg shadow">
            <table class="w-full table-auto">
                <thead class="bg-gray-200 text-gray-600 uppercase text-sm leading-normal">
                    <tr>
                        <th class="py-3 px-6 text-left">Name</th>
                        <th class="py-2 px-4 text-left">Email</th>
                        <th class="py-3 px-6 text-left">Phone</th>
                        <th class="py-3 px-6 text-left">Subjects</th>
                        @hasanyrole('Admin|rector|frontdesk')
                        <th class="py-3 px-6 text-center">Actions</th>
                        @endhasanyrole
                        <!-- @hasrole('frontdesk')
                        <th class="py-3 px-6 text-center">Profile</th>
                        @endhasrole -->
                    </tr>
                </thead>
                <tbody class="text-gray-600 text-sm font-light">
                    @forelse ($teachers as $teacher)
                    <tr class="border-b border-gray-200 hover:bg-gray-100">
                        <td class="py-3 px-6 text-left whitespace-nowrap">{{ $teacher->user->name }}</td>
                        <td class="py-2 px-4 text-left">{{ $teacher->user->email }}</td>
                        <td class="py-3 px-6 text-left">{{ $teacher->phone }}</td>
                        <td class="py-3 px-6 text-left">
                            @forelse ($teacher->subjects as $subject)
                                {{ $subject->subject_name }}@if (!$loop->last), @endif
                            @empty
                                No subjects found
                            @endforelse
                        </td>
                        <td class="py-3 px-6 text-center">
                            <a href="{{ route('teacher.profile', $teacher->id) }}" class="text-blue-600 hover:underline">Profile</a>
                            @hasanyrole('Admin|rector|frontdesk')
                            <a href="{{ route('assign.subject', $teacher->id) }}" class="ml-4 text-green-600 hover:underline">Assign Subject</a>
                            @hasrole('Admin|rector')
                            <a href="{{ route('deleteassigned.subject', $teacher->id) }}" class="ml-4 text-blue-600 hover:underline">Delete Assigned Subject(s)</a>
                            <form action="{{route('teacher.delete', $teacher->id)}}" method="POST" class="inline">
                                @csrf
                                @method('DELETE')
                                <button type="submit" class="ml-4 text-red-600 hover:underline">Delete</button>
                            </form>
                            @endhasrole
                            @endhasanyrole
                            @if (session('success'))
                                <script>
                                    Swal.fire({
                                        title: 'Success!',
                                        text: '{{ session('success') }}',
                                        icon: 'success',
                                        confirmButtonText: 'OK'
                                    });
                                </script>
                            @endif 
                        </td>
                    </tr>
                    @empty
                    <tr>
                        <td colspan="5" class="py-3 px-6 text-center text-gray-500">No teachers found.</td>
                    </tr>
                    @endforelse
                </tbody>
            </table>
        </div>
    
        <!-- Pagination -->
        <div class="mt-8">
            {{ $teachers->links() }}
        </div>

        @include('backend.modals.delete',['name' => 'teacher'])
    </div>
@endsection

@push('scripts')
<script>
    $(function() {
        $(".deletebtn").on("click", function(event) {
            event.preventDefault();
            $("#deletemodal").toggleClass("hidden");
            var url = $(this).attr('data-url');
            $(".remove-record").attr("action", url);
        });
        
        $("#deletemodelclose").on("click", function(event) {
            event.preventDefault();
            $("#deletemodal").toggleClass("hidden");
        });
    });
</script>
@endpush
